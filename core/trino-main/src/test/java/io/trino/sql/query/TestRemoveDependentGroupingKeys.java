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
public class TestRemoveDependentGroupingKeys
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
        String plan = (String) assertions.execute("EXPLAIN SELECT x, x - 1, 1, count(*) FROM (VALUES 1, 2) t(x) GROUP BY x, x - 1, 1").getOnlyValue();
        assertThat(plan).containsPattern("keys = \\[[a-z_0-9]+\\]");
    }

    @Test
    public void testConstantKey()
    {
        assertThat(assertions.query(
                """
                SELECT 1, x, count(*)
                FROM (VALUES 'a', 'b', 'a', NULL) t(x)
                GROUP BY 1, x
                """))
                .matches("VALUES (1, 'a', BIGINT '2'), (1, 'b', BIGINT '1'), (1, NULL, BIGINT '1')");

        assertThat(assertions.query(
                """
                SELECT 1, x, count(*)
                FROM (VALUES 'a') t(x)
                WHERE x = 'b'
                GROUP BY 1, x
                """))
                .returnsEmptyResult();
    }

    @Test
    public void testDependentKeys()
    {
        assertThat(assertions.query(
                """
                SELECT x, x - 1, x + 2, x = 2, count(*)
                FROM (VALUES 1, 2, 2, NULL) t(x)
                GROUP BY x, x - 1, x + 2, x = 2
                """))
                .matches("VALUES (1, 0, 3, false, BIGINT '1'), (2, 1, 4, true, BIGINT '2'), (NULL, NULL, NULL, NULL, BIGINT '1')");

        assertThat(assertions.query(
                """
                SELECT x, y, upper(x), length(y), count(*)
                FROM (VALUES ('a', 'bb'), ('a', 'bb'), ('c', 'd')) t(x, y)
                GROUP BY x, y, upper(x), length(y)
                """))
                .matches("VALUES ('a', 'bb', 'A', BIGINT '2', BIGINT '2'), ('c', 'd', 'C', BIGINT '1', BIGINT '1')");
    }

    @Test
    public void testDistinguishableEqualValues()
    {
        // 0.0 and -0.0 are one group, but they divide 1 into different values
        assertThat(assertions.query(
                """
                SELECT 1 / x, count(*)
                FROM (VALUES DOUBLE '0.0', DOUBLE '-0.0') t(x)
                GROUP BY x, 1 / x
                """))
                .matches("VALUES (DOUBLE 'Infinity', BIGINT '1'), (DOUBLE '-Infinity', BIGINT '1')");
    }

    @Test
    public void testOverflow()
    {
        assertThat(assertions.query(
                """
                SELECT x, x - 1, x + 1, count(*)
                FROM (VALUES 2147483646, -2147483647) t(x)
                GROUP BY x, x - 1, x + 1
                """))
                .matches("VALUES (2147483646, 2147483645, 2147483647, BIGINT '1'), (-2147483647, -2147483648, -2147483646, BIGINT '1')");

        assertThat(assertions.query(
                """
                SELECT x, x + 1, count(*)
                FROM (VALUES 1, 2147483647) t(x)
                GROUP BY x, x + 1
                ORDER BY count(*) DESC, x
                LIMIT 1
                """))
                .failure().hasMessage("integer addition overflow: 2147483647 + 1");

        assertThat(assertions.query(
                """
                SELECT x, x - 1, count(*)
                FROM (VALUES 1, -2147483648) t(x)
                GROUP BY x, x - 1
                HAVING count(*) > 5
                """))
                .failure().hasMessage("integer subtraction overflow: -2147483648 - 1");
    }
}
