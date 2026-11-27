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
public class TestPullConstantOffsetOutOfSum
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testRuleFires()
    {
        String plan = (String) assertions.execute("EXPLAIN SELECT sum(x + 1), sum(x + 2) FROM (VALUES 1, 2) t(x)").getOnlyValue();
        assertThat(plan)
                .contains("count(")
                .doesNotContain("$operator$add(x, integer '1')");
    }

    @Test
    public void testGlobalAggregation()
    {
        assertThat(assertions.query(
                """
                SELECT sum(x), sum(x + 1), sum(2 + x), sum(x - 3), sum(x + 0)
                FROM (VALUES 1, 2, NULL, 4) t(x)
                """))
                .matches("VALUES (BIGINT '7', BIGINT '10', BIGINT '13', BIGINT '-2', BIGINT '7')");

        assertThat(assertions.query(
                """
                SELECT sum(x), sum(x + 1), sum(x - 3)
                FROM (VALUES CAST(NULL AS integer), NULL) t(x)
                """))
                .matches("VALUES (CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint))");

        assertThat(assertions.query(
                """
                SELECT sum(x), sum(x + 1), sum(x - 3)
                FROM (VALUES 1, 2) t(x)
                WHERE x > 5
                """))
                .matches("VALUES (CAST(NULL AS bigint), CAST(NULL AS bigint), CAST(NULL AS bigint))");
    }

    @Test
    public void testGroupedAggregation()
    {
        assertThat(assertions.query(
                """
                SELECT k, sum(x + 1), sum(x + 10), count(*)
                FROM (VALUES ('a', 1), ('a', 2), ('b', NULL), ('c', 5), ('c', NULL)) t(k, x)
                GROUP BY k
                """))
                .matches(
                        """
                        VALUES
                            ('a', BIGINT '5', BIGINT '23', BIGINT '2'),
                            ('b', NULL, NULL, BIGINT '1'),
                            ('c', BIGINT '6', BIGINT '15', BIGINT '2')
                        """);

        assertThat(assertions.query(
                """
                SELECT k, sum(x + 1), sum(x + 10)
                FROM (VALUES ('a', 1), ('a', 2), ('b', 3)) t(k, x)
                GROUP BY GROUPING SETS ((k), ())
                """))
                .matches(
                        """
                        VALUES
                            ('a', BIGINT '5', BIGINT '23'),
                            ('b', BIGINT '4', BIGINT '13'),
                            (NULL, BIGINT '9', BIGINT '36')
                        """);
    }

    @Test
    public void testFilter()
    {
        assertThat(assertions.query(
                """
                SELECT sum(x + 1) FILTER (WHERE x > 1), sum(x + 2) FILTER (WHERE x > 1), sum(x + 3)
                FROM (VALUES 1, 2, 3) t(x)
                """))
                .matches("VALUES (BIGINT '7', BIGINT '9', BIGINT '15')");
    }

    @Test
    public void testNarrowTypes()
    {
        assertThat(assertions.query(
                """
                SELECT sum(x + TINYINT '1'), sum(x - TINYINT '100')
                FROM (VALUES TINYINT '-28', TINYINT '126') t(x)
                """))
                .matches("VALUES (BIGINT '100', BIGINT '-102')");

        assertThat(assertions.query(
                """
                SELECT sum(x + SMALLINT '1'), sum(x + SMALLINT '2')
                FROM (VALUES SMALLINT '32765', SMALLINT '-32768') t(x)
                """))
                .matches("VALUES (BIGINT '-1', BIGINT '1')");
    }

    @Test
    public void testOverflow()
    {
        assertThat(assertions.query(
                """
                SELECT sum(x), sum(x + 1), sum(x + 2)
                FROM (VALUES 2147483644, 2147483645) t(x)
                """))
                .matches("VALUES (BIGINT '4294967289', BIGINT '4294967291', BIGINT '4294967293')");

        assertThat(assertions.query(
                """
                SELECT sum(x), sum(x + 1), sum(x + 2)
                FROM (VALUES 0, 2147483646) t(x)
                """))
                .failure().hasMessage("integer addition overflow: 2147483646 + 2");

        assertThat(assertions.query(
                """
                SELECT sum(x), sum(x - 1), sum(x - 2)
                FROM (VALUES 0, -2147483647) t(x)
                """))
                .failure().hasMessage("integer subtraction overflow: -2147483647 - 2");

        assertThat(assertions.query(
                """
                SELECT sum(x + TINYINT '1'), sum(x + TINYINT '2')
                FROM (VALUES TINYINT '126') t(x)
                """))
                .failure().hasMessage("tinyint addition overflow: 126 + 2");

        assertThat(assertions.query(
                """
                SELECT count(*), sum(x + 1), sum(x + 2)
                FROM (VALUES (0, 1), (2147483647, 2)) t(x, k)
                GROUP BY k
                HAVING count(*) > 5
                """))
                .failure().hasMessage("integer addition overflow: 2147483647 + 2");
    }
}
