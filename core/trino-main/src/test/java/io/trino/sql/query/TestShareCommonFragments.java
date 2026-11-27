/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.SHARE_COMMON_FRAGMENTS;
import static io.trino.SystemSessionProperties.USE_TABLE_SCAN_NODE_PARTITIONING;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestShareCommonFragments
{
    private static final Pattern FRAGMENT = Pattern.compile("(?m)^Fragment \\d+");

    private final QueryAssertions assertions;
    private final Session shared;
    private final Session notShared;

    public TestShareCommonFragments()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                // read the copies of a common table expression through remote exchanges
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(USE_TABLE_SCAN_NODE_PARTITIONING, "false")
                .build();
        shared = Session.builder(session)
                .setSystemProperty(SHARE_COMMON_FRAGMENTS, "true")
                .build();
        notShared = Session.builder(session)
                .setSystemProperty(SHARE_COMMON_FRAGMENTS, "false")
                .build();

        // small buffers, so that consumers of a shared fragment that wait for each other would deadlock
        // if the producer blocked on a full output buffer
        QueryRunner runner = new StandaloneQueryRunner(session, builder -> builder
                .addProperty("sink.max-buffer-size", "1kB")
                .addProperty("sink.max-broadcast-buffer-size", "1kB"));
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog(TEST_CATALOG_NAME, "tpch", ImmutableMap.of("tpch.splits-per-node", "4"));
        assertions = new QueryAssertions(runner);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSelfJoinOfAggregation()
    {
        // build and probe side of the join read the same shared fragment, dynamic filters from the joins are on the small dimension tables
        assertShared(
                """
                WITH t AS (
                    SELECT n.name, c.mktsegment, sum(o.totalprice) total, count(*) orders
                    FROM orders o, customer c, nation n
                    WHERE o.custkey = c.custkey AND c.nationkey = n.nationkey
                    GROUP BY n.name, c.mktsegment)
                SELECT a.name, a.mktsegment, a.total, a.orders, b.total, b.orders
                FROM t a JOIN t b ON a.name = b.name AND a.mktsegment < b.mktsegment
                """);

        // without dynamic filters
        assertShared(
                withoutDynamicFiltering(),
                """
                WITH t AS (SELECT orderkey % 5000 k, sum(quantity) quantity, count(*) items FROM lineitem GROUP BY orderkey % 5000)
                SELECT a.k, a.quantity, a.items, b.quantity, b.items
                FROM t a JOIN t b ON a.k = b.k + 1
                """);

        assertShared(
                withoutDynamicFiltering(),
                """
                WITH t AS (SELECT suppkey, partkey, sum(quantity) quantity FROM lineitem GROUP BY suppkey, partkey)
                SELECT count(*), sum(a.quantity), sum(b.quantity), sum(c.quantity)
                FROM t a, t b, t c
                WHERE a.suppkey = b.suppkey AND a.partkey = b.partkey + 1 AND b.suppkey = c.suppkey AND b.partkey = c.partkey + 1
                """);
    }

    @Test
    public void testSelfJoinOfScanWithDynamicFilter()
    {
        @Language("SQL") String sql = """
                WITH t AS (SELECT orderkey % 1000 k, linenumber, comment FROM lineitem)
                SELECT count(*), max(a.comment), min(b.comment)
                FROM t a JOIN t b ON a.k = b.k
                WHERE a.linenumber < b.linenumber
                """;
        // only the probe side copy is filtered with a dynamic filter, so the shared copy computes what the build side copy computes anyway
        assertShared(sql);
        // the shared scan output is larger than the output buffers
        assertShared(withoutDynamicFiltering(), sql);
    }

    @Test
    public void testNotSharedWhenEveryCopyLosesDynamicFilter()
    {
        // each copy of the scan is filtered with a dynamic filter from a different join, a shared copy would read all rows
        assertNotShared(
                """
                WITH t AS (SELECT orderkey, sum(quantity) quantity FROM lineitem GROUP BY orderkey)
                SELECT count(*), sum(t.quantity) FROM t JOIN orders o ON t.orderkey = o.orderkey WHERE o.orderpriority = '1-URGENT'
                UNION ALL
                SELECT count(*), sum(t.quantity) FROM t JOIN orders o ON t.orderkey = o.orderkey WHERE o.orderstatus = 'P'
                """);
    }

    @Test
    public void testSelfJoinOfWindow()
    {
        assertShared(
                withoutDynamicFiltering(),
                """
                WITH t AS (
                    SELECT custkey, orderdate, totalprice, rank() OVER (PARTITION BY custkey ORDER BY orderdate, orderkey) rn
                    FROM orders)
                SELECT a.custkey, a.orderdate, b.orderdate, a.totalprice - b.totalprice
                FROM t a JOIN t b ON a.custkey = b.custkey AND a.rn = b.rn + 1
                """);
    }

    @Test
    public void testSharedInDifferentConsumers()
    {
        // the shared aggregation is read by two different stages
        assertShared(
                """
                WITH t AS (SELECT custkey, sum(totalprice) total FROM orders GROUP BY custkey)
                SELECT 'high', count(*) FROM t WHERE total > (SELECT avg(total) FROM t)
                UNION ALL
                SELECT 'low', count(*) FROM t WHERE total <= (SELECT avg(total) FROM t)
                """);

        assertShared(
                """
                WITH t AS (SELECT suppkey FROM lineitem WHERE quantity > 45 GROUP BY suppkey)
                SELECT count(*) FROM (
                    SELECT orderkey FROM orders WHERE custkey IN (SELECT suppkey FROM t)
                    UNION ALL
                    SELECT partkey FROM part WHERE partkey IN (SELECT suppkey FROM t))
                """);
    }

    @Test
    public void testLeftJoinAndSemiJoin()
    {
        assertShared(
                """
                WITH t AS (SELECT custkey, count(*) orders FROM orders WHERE orderstatus = 'F' GROUP BY custkey)
                SELECT c.custkey, a.orders, b.orders
                FROM customer c
                LEFT JOIN t a ON c.custkey = a.custkey
                LEFT JOIN t b ON c.custkey = b.custkey + 1
                """);
    }

    @Test
    public void testNotSharedWhenDifferent()
    {
        // the copies differ in their filters, so they are not shared
        assertNotShared(
                """
                WITH t AS (SELECT custkey, orderstatus, sum(totalprice) total FROM orders GROUP BY custkey, orderstatus)
                SELECT a.custkey, a.total, b.total
                FROM t a JOIN t b ON a.custkey = b.custkey
                WHERE a.orderstatus = 'F' AND b.orderstatus = 'O'
                """);
    }

    @Test
    public void testNotSharedWhenNonDeterministic()
    {
        assertNotShared(
                """
                WITH t AS (SELECT orderkey % 5000 k, sum(quantity * random()) quantity FROM lineitem GROUP BY orderkey % 5000)
                SELECT count(*)
                FROM t a JOIN t b ON a.k = b.k + 1
                WHERE a.quantity < b.quantity
                """);
    }

    private void assertShared(@Language("SQL") String sql)
    {
        assertShared(Session.builder(shared).build(), sql);
    }

    private void assertShared(Session session, @Language("SQL") String sql)
    {
        Session sharedSession = Session.builder(session).setSystemProperty(SHARE_COMMON_FRAGMENTS, "true").build();
        Session notSharedSession = Session.builder(session).setSystemProperty(SHARE_COMMON_FRAGMENTS, "false").build();
        assertThat(fragmentCount(sharedSession, sql)).isLessThan(fragmentCount(notSharedSession, sql));
        MaterializedResult expected = assertions.execute(notSharedSession, sql);
        MaterializedResult actual = assertions.execute(sharedSession, sql);
        assertThat(actual.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
    }

    private Session withoutDynamicFiltering()
    {
        return Session.builder(shared)
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
                .build();
    }

    private void assertNotShared(@Language("SQL") String sql)
    {
        assertThat(fragmentCount(shared, sql)).isEqualTo(fragmentCount(notShared, sql));
    }

    private int fragmentCount(Session session, @Language("SQL") String sql)
    {
        String plan = (String) assertions.execute(session, "EXPLAIN (TYPE DISTRIBUTED) " + sql).getOnlyValue();
        Matcher matcher = FRAGMENT.matcher(plan);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}
