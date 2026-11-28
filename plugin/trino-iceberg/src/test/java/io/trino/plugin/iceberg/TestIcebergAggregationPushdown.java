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
package io.trino.plugin.iceberg;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergAggregationPushdown
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().build();
    }

    @Test
    public void testGlobalAggregationsFromFileStatistics()
    {
        try (TestTable table = newTrinoTable(
                "test_aggregation_pushdown",
                "(i integer, b bigint, d date, s varchar, r double, dec decimal(10, 2), ldec decimal(30, 5), ts timestamp(6), tstz timestamp(6) with time zone)")) {
            String name = table.getName();
            // empty table
            assertPushedDown("SELECT count(*), count(i), min(i), max(b), min(d) FROM " + name);

            assertUpdate("INSERT INTO " + name + " VALUES " +
                    "(1, 10, DATE '2020-01-01', 'a', 1.0, 12.34, 123456789012345678901.12345, TIMESTAMP '2020-01-01 10:00:00.123456', TIMESTAMP '2020-01-01 10:00:00.123456 UTC'), " +
                    "(NULL, -5, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", 2);
            assertUpdate("INSERT INTO " + name + " VALUES " +
                    "(-7, NULL, DATE '2019-05-03', 'b', 2.0, -0.01, -1.5, TIMESTAMP '1969-12-31 23:59:59.999999', TIMESTAMP '2021-06-01 00:00:00.000001 +02:00'), " +
                    "(3, 42, DATE '2021-12-31', 'c', nan(), 99999999.99, 0, TIMESTAMP '2030-01-01 00:00:00', TIMESTAMP '1970-01-01 00:00:00 UTC')", 2);
            // a file with only nulls in some columns
            assertUpdate("INSERT INTO " + name + " VALUES (NULL, 7, NULL, NULL, NULL, NULL, NULL, NULL, NULL)", 1);

            assertPushedDown("SELECT count(*), count(i), count(s), min(i), max(i), min(b), max(b), min(d), max(d) FROM " + name);
            assertPushedDown("SELECT min(dec), max(dec), min(ldec), max(ldec), min(ts), max(ts), min(tstz), max(tstz) FROM " + name);
            assertPushedDown("SELECT count(*) FROM " + name);
            assertPushedDown("SELECT max(d) FROM " + name);
            assertQuery("SELECT count(*), count(i), min(i), max(i), max(b), min(d) FROM " + name, "VALUES (5, 3, -7, 3, 42, DATE '2019-05-03')");

            // bounds of strings may be truncated, floating point bounds do not account for NaN
            assertNotPushedDown("SELECT min(s) FROM " + name);
            assertNotPushedDown("SELECT max(r) FROM " + name);
            assertNotPushedDown("SELECT count(r) FROM " + name);
            assertNotPushedDown("SELECT min(i), sum(i) FROM " + name);
            assertNotPushedDown("SELECT count(DISTINCT i) FROM " + name);
            assertNotPushedDown("SELECT max(i) FILTER (WHERE b > 0) FROM " + name);
            assertNotPushedDown("SELECT max(i) FROM " + name + " WHERE b > 0");
            assertNotPushedDown("SELECT b, max(i) FROM " + name + " GROUP BY b");

            // deleted rows are not reflected in file statistics
            assertUpdate("DELETE FROM " + name + " WHERE i = 3", 1);
            assertNotPushedDown("SELECT count(*), max(i) FROM " + name);
            assertNotPushedDown("SELECT count(b) FROM " + name);
            assertQuery("SELECT count(*), count(i), max(i), max(b) FROM " + name, "VALUES (4, 2, 1, 10)");

            // rewriting the files removes the deletes
            assertUpdate("ALTER TABLE " + name + " EXECUTE optimize");
            assertPushedDown("SELECT count(*), count(i), max(i), max(b) FROM " + name);
            assertQuery("SELECT count(*), count(i), max(i), max(b) FROM " + name, "VALUES (4, 2, 1, 10)");
        }
    }

    @Test
    public void testPartitionedTable()
    {
        try (TestTable table = newTrinoTable("test_aggregation_pushdown_partitioned", "(i integer, p integer) WITH (partitioning = ARRAY['p'])")) {
            String name = table.getName();
            assertUpdate("INSERT INTO " + name + " VALUES (1, 1), (2, 1), (30, 2), (NULL, 2), (-4, 3)", 5);

            assertPushedDown("SELECT count(*), count(i), min(i), max(i) FROM " + name + " WHERE p = 2");
            assertPushedDown("SELECT count(*), min(i), max(i) FROM " + name + " WHERE p IN (1, 3)");
            assertPushedDown("SELECT count(*), min(i), max(i) FROM " + name + " WHERE p = 5");
            assertQuery("SELECT count(*), count(i), min(i), max(i) FROM " + name + " WHERE p = 2", "VALUES (2, 1, 30, 30)");
            // a predicate on a non-partition column may select only some rows of a file
            assertNotPushedDown("SELECT count(*), max(i) FROM " + name + " WHERE p = 2 AND i > 0");
        }
    }

    private void assertPushedDown(@Language("SQL") String sql)
    {
        assertThat(explain(sql)).doesNotContain("Scan").contains("Values");
        assertThat(query(sql)).hasCorrectResultsRegardlessOfPushdown();
    }

    private void assertNotPushedDown(@Language("SQL") String sql)
    {
        assertThat(explain(sql)).contains("Scan");
        assertThat(query(sql)).hasCorrectResultsRegardlessOfPushdown();
    }

    private String explain(@Language("SQL") String sql)
    {
        return (String) computeActual("EXPLAIN " + sql).getOnlyValue();
    }
}
