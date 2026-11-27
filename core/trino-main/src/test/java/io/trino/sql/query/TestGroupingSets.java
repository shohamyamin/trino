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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGroupingSets
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testPredicateOverGroupingKeysWithEmptyGroupingSet()
    {
        assertThat(assertions.query(
                "WITH t AS (" +
                        "    SELECT a" +
                        "    FROM (" +
                        "        VALUES 1, 2" +
                        "    ) AS u(a)" +
                        "    GROUP BY GROUPING SETS ((), (a))" +
                        ")" +
                        "SELECT * " +
                        "FROM t " +
                        "WHERE a IS NOT NULL"))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testRollupWithRepeatedGroups()
    {
        // Several input rows per group exercise pre-aggregation below GroupId
        assertThat(assertions.query(
                "SELECT a, b, grouping(a, b), count(*), count(x), sum(x), avg(x), max(s), sum(a), count(x) FILTER (WHERE x > 1) " +
                        "FROM (VALUES " +
                        "    (1, 'p', 1, 'k'), (1, 'p', 2, 'l'), (1, 'q', NULL, 'm'), (1, 'q', 3, NULL), " +
                        "    (2, 'p', 4, 'n'), (2, 'p', 5, 'o'), (NULL, 'p', 6, 'p'), (NULL, NULL, 7, 'q'), (NULL, NULL, NULL, 'r')) t(a, b, x, s) " +
                        "GROUP BY ROLLUP (a, b)"))
                .matches("VALUES " +
                        "(1, 'p', 0, BIGINT '2', BIGINT '2', BIGINT '3', 1.5e0, 'l', BIGINT '2', BIGINT '1'), " +
                        "(1, 'q', 0, 2, 1, 3, 3.0e0, 'm', 2, 1), " +
                        "(2, 'p', 0, 2, 2, 9, 4.5e0, 'o', 4, 2), " +
                        "(NULL, 'p', 0, 1, 1, 6, 6.0e0, 'p', NULL, 1), " +
                        "(NULL, NULL, 0, 2, 1, 7, 7.0e0, 'r', NULL, 1), " +
                        "(1, NULL, 1, 4, 3, 6, 2.0e0, 'm', 4, 2), " +
                        "(2, NULL, 1, 2, 2, 9, 4.5e0, 'o', 4, 2), " +
                        "(NULL, NULL, 1, 3, 2, 13, 6.5e0, 'r', NULL, 2), " +
                        "(NULL, NULL, 3, 9, 7, 28, 4.0e0, 'r', 8, 6)");

        assertThat(assertions.query(
                "SELECT a, b, count(*), sum(x) " +
                        "FROM (VALUES (1, 1, 1), (1, 1, 2), (1, 2, 3), (2, 1, 4), (2, 1, 5)) t(a, b, x) " +
                        "GROUP BY CUBE (a, b)"))
                .matches("VALUES " +
                        "(1, 1, BIGINT '2', BIGINT '3'), (1, 2, 1, 3), (2, 1, 2, 9), " +
                        "(1, NULL, 3, 6), (2, NULL, 2, 9), " +
                        "(NULL, 1, 4, 12), (NULL, 2, 1, 3), " +
                        "(NULL, NULL, 5, 15)");
    }

    @Test
    public void testDistinctWithMixedReferences()
    {
        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 1) t(a) " +
                "GROUP BY DISTINCT ROLLUP(a, t.a)"))
                .matches("VALUES (1), (NULL)");

        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 1) t(a) " +
                "GROUP BY DISTINCT GROUPING SETS ((a), (t.a))"))
                .matches("VALUES 1");

        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 1) t(a) " +
                "GROUP BY DISTINCT a, GROUPING SETS ((), (t.a))"))
                .matches("VALUES 1");
    }

    @Test
    public void testRollupAggregationWithOrderedLimit()
    {
        assertThat(assertions.query("" +
                "SELECT a " +
                "FROM (VALUES 3, 2, 1) t(a) " +
                "GROUP BY ROLLUP (a) " +
                "ORDER BY a LIMIT 2"))
                .matches("VALUES 1, 2");
    }

    @Test
    public void testComplexCube()
    {
        assertThat(assertions.query(
                """
                SELECT a, b, c, count(*)
                FROM (VALUES (1, 1, 1), (1, 2, 2), (1, 2, 2)) t(a, b, c)
                GROUP BY CUBE (a, (b, c))
                """))
                .matches(
                        """
                        VALUES
                            (   1,    1,    1, BIGINT '1'),
                            (   1,    2,    2, 2),
                            (   1, NULL, NULL, 3),
                            (NULL, NULL, NULL, 3),
                            (NULL,    1,    1, 1),
                            (NULL,    2,    2, 2)
                        """);
    }

    @Test
    public void testComplexRollup()
    {
        assertThat(assertions.query(
                """
                SELECT a, b, c, count(*)
                FROM (VALUES (1, 1, 1), (1, 2, 2), (1, 2, 2)) t(a, b, c)
                GROUP BY ROLLUP (a, (b, c))
                """))
                .matches(
                        """
                         VALUES
                         (   1,    1,    1, BIGINT '1'),
                         (NULL, NULL, NULL, 3),
                         (   1, NULL, NULL, 3),
                         (   1,    2,    2, 2)
                        """);
    }
}
