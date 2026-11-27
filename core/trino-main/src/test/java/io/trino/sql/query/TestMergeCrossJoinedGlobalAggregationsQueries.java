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

import static io.trino.SystemSessionProperties.MERGE_CROSS_JOINED_GLOBAL_AGGREGATIONS;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestMergeCrossJoinedGlobalAggregationsQueries
{
    private final QueryAssertions assertions;
    private final Session merged;
    private final Session notMerged;

    public TestMergeCrossJoinedGlobalAggregationsQueries()
    {
        Session session = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TINY_SCHEMA_NAME)
                .build();
        merged = Session.builder(session)
                .setSystemProperty(MERGE_CROSS_JOINED_GLOBAL_AGGREGATIONS, "true")
                .build();
        notMerged = Session.builder(session)
                .setSystemProperty(MERGE_CROSS_JOINED_GLOBAL_AGGREGATIONS, "false")
                .build();
        QueryRunner runner = new StandaloneQueryRunner(session);
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
    public void testScalarSubqueries()
    {
        assertSameResults(
                """
                SELECT
                    (SELECT count(*) FROM lineitem WHERE quantity BETWEEN 1 AND 20),
                    (SELECT avg(extendedprice) FROM lineitem WHERE quantity BETWEEN 1 AND 20),
                    (SELECT avg(discount) FROM lineitem WHERE quantity BETWEEN 21 AND 40),
                    (SELECT sum(tax) FROM lineitem),
                    (SELECT max(comment) FROM lineitem WHERE shipmode = 'AIR')
                """);

        assertSameResults(
                """
                SELECT r_name, CASE WHEN (SELECT count(*) FROM lineitem WHERE quantity < 10) > 1000
                    THEN (SELECT avg(extendedprice) FROM lineitem WHERE quantity < 10)
                    ELSE (SELECT avg(extendedprice) FROM lineitem WHERE quantity >= 10) END
                FROM (SELECT name r_name FROM region)
                """);
    }

    @Test
    public void testNoMatchingRows()
    {
        assertThat(assertions.query(merged,
                """
                SELECT
                    (SELECT count(*) FROM orders WHERE totalprice < 0),
                    (SELECT sum(totalprice) FROM orders WHERE totalprice < 0),
                    (SELECT count(*) FROM orders WHERE orderstatus = 'F')
                """))
                .matches("VALUES (BIGINT '0', CAST(NULL AS double), BIGINT '7304')");
    }

    @Test
    public void testNullPredicates()
    {
        assertSameResults(
                """
                SELECT
                    (SELECT count(*) FROM orders WHERE nullif(orderstatus, 'F') = 'O'),
                    (SELECT count(*) FROM orders WHERE nullif(orderstatus, 'O') <> 'O'),
                    (SELECT count(orderkey) FROM orders)
                """);
    }

    @Test
    public void testProjectionsOfOtherRowsAreNotEvaluated()
    {
        // 1 / (quantity - 1) fails for the rows of the other subquery
        assertSameResults(
                """
                SELECT
                    (SELECT sum(1 / (quantity - 1)) FROM lineitem WHERE quantity > 1),
                    (SELECT count(*) FROM lineitem WHERE quantity = 1)
                """);
    }

    @Test
    public void testDistinctAggregations()
    {
        assertSameResults(
                """
                SELECT
                    (SELECT count(DISTINCT partkey) FROM lineitem WHERE quantity < 10),
                    (SELECT count(DISTINCT partkey) FROM lineitem WHERE quantity >= 40),
                    (SELECT count(DISTINCT suppkey) FROM lineitem)
                """);
    }

    @Test
    public void testSubqueriesInFilter()
    {
        assertSameResults(
                """
                SELECT count(*)
                FROM orders
                WHERE totalprice > (SELECT avg(totalprice) FROM orders WHERE orderstatus = 'F')
                    AND totalprice < (SELECT max(totalprice) FROM orders WHERE orderstatus = 'O')
                """);
    }

    private void assertSameResults(@Language("SQL") String sql)
    {
        MaterializedResult expected = assertions.execute(notMerged, sql);
        MaterializedResult actual = assertions.execute(merged, sql);
        assertThat(actual.getMaterializedRows()).containsExactlyInAnyOrderElementsOf(expected.getMaterializedRows());
    }
}
