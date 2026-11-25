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

import java.math.BigInteger;

import static java.lang.String.format;
import static java.math.BigInteger.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestWindowFrameRows
{
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSlidingVariance()
    {
        assertThat(assertions.query(
                "SELECT i, variance(x) OVER w, var_pop(x) OVER w, stddev_pop(x) OVER w " +
                        "FROM (VALUES (1, 1e0), (2, 3e0), (3, 7e0), (4, 7e0), (5, 10e0)) t(i, x) " +
                        "WINDOW w AS (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)"))
                .matches("VALUES " +
                        "(1, CAST(null AS double), 0e0, 0e0), " +
                        "(2, 2e0, 1e0, 1e0), " +
                        "(3, 8e0, 4e0, 2e0), " +
                        "(4, 0e0, 0e0, 0e0), " +
                        "(5, 4.5e0, 2.25e0, 1.5e0)");

        assertThat(assertions.query(
                "SELECT i, stddev(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES (1, BIGINT '1'), (2, BIGINT '3'), (3, BIGINT '7'), (4, BIGINT '7'), (5, BIGINT '10')) t(i, x)"))
                .matches("VALUES (1, CAST(null AS double)), (2, sqrt(2e0)), (3, sqrt(8e0)), (4, 0e0), (5, sqrt(4.5e0))");

        // the frame is re-aggregated after a non-finite value leaves it, and nulls are ignored
        assertThat(assertions.query(
                "SELECT i, variance(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) " +
                        "FROM (VALUES (1, 1e0), (2, nan()), (3, 5e0), (4, 9e0), (5, infinity()), (6, CAST(null AS double)), (7, 2e0), (8, 4e0)) t(i, x)"))
                .matches("VALUES " +
                        "(1, CAST(null AS double)), " +
                        "(2, nan()), " +
                        "(3, nan()), " +
                        "(4, 8e0), " +
                        "(5, nan()), " +
                        "(6, CAST(null AS double)), " +
                        "(7, CAST(null AS double)), " +
                        "(8, 2e0)");
    }

    @Test
    public void testSlidingStandardDeviationMatchesAggregation()
    {
        // a large mean and a small spread stress the numerical stability of removing rows from the frame
        assertThat(assertions.query(
                "WITH data AS (SELECT i, 1e6 + 3 * sin(i) AS x FROM UNNEST(sequence(1, 5000)) t(i)), " +
                        "sliding AS (SELECT i, stddev(x) OVER (ORDER BY i ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS deviation FROM data), " +
                        "aggregated AS (SELECT d1.i, stddev(d2.x) AS deviation FROM data d1 JOIN data d2 ON d2.i BETWEEN d1.i - 99 AND d1.i GROUP BY d1.i) " +
                        "SELECT count(*) FROM sliding JOIN aggregated USING (i) " +
                        "WHERE NOT (abs(sliding.deviation - aggregated.deviation) <= 1e-9 * aggregated.deviation OR (sliding.deviation IS NULL AND aggregated.deviation IS NULL))"))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testOffsetTypes()
    {
        String expected = "VALUES " +
                "ARRAY[null, null, 1], " +
                "ARRAY[null, null, 1, 2], " +
                "ARRAY[null, 1, 2, 2], " +
                "ARRAY[1, 2, 2], " +
                "ARRAY[2, 2]";

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN TINYINT '1' PRECEDING AND TINYINT '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN SMALLINT '1' PRECEDING AND SMALLINT '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN INTEGER '1' PRECEDING AND INTEGER '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN BIGINT '1' PRECEDING AND BIGINT '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        // short decimal
        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN DECIMAL '1' PRECEDING AND DECIMAL '2' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);

        expected = "VALUES " +
                "ARRAY[null, null, 1, 2, 2], " +
                "ARRAY[null, null, 1, 2, 2], " +
                "ARRAY[null, 1, 2, 2], " +
                "ARRAY[1, 2, 2], " +
                "ARRAY[2, 2]";

        // short decimal: no integer overflow exception when frame offset exceeds integer
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '%d' FOLLOWING) " +
                        "FROM (VALUES 2, 2, 1, null, null) t(a)",
                1L + Integer.MAX_VALUE)))
                .matches(expected);

        // long decimal: value does not overflow long
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '%d' FOLLOWING) " +
                        "FROM (VALUES 2, 2, 1, null, null) t(a)",
                Long.MAX_VALUE)))
                .matches(expected);

        // long decimal: value overflows long so it is truncated to max long
        assertThat(assertions.query(format(
                "SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '%s' FOLLOWING) " +
                        "FROM (VALUES 2, 2, 1, null, null) t(a)",
                BigInteger.valueOf(Long.MAX_VALUE).add(ONE))))
                .matches(expected);

        assertThat(assertions.query("SELECT array_agg(a) OVER(ORDER BY a ASC NULLS FIRST ROWS BETWEEN 1 PRECEDING AND DECIMAL '999999999999999999999999999999' FOLLOWING) " +
                "FROM (VALUES 2, 2, 1, null, null) t(a)"))
                .matches(expected);
    }

    @Test
    public void testEmptyFrame()
    {
        // PRECEDING to PRECEDING: both offsets before partition start
        assertThat(assertions.query("SELECT a, array_agg(a) OVER(ORDER BY a ROWS BETWEEN start_offset PRECEDING AND end_offset PRECEDING) " +
                "FROM (VALUES (1, 0, 0), (2, 5, 3)) t(a, start_offset, end_offset)"))
                .matches("VALUES (1, ARRAY[1]), (2, null)");

        // FOLLOWING to FOLLOWING: start offset past partition end
        assertThat(assertions.query("SELECT a, array_agg(a) OVER(ORDER BY a ROWS BETWEEN start_offset FOLLOWING AND end_offset FOLLOWING) " +
                "FROM (VALUES (1, 0, 0), (2, 3, 5)) t(a, start_offset, end_offset)"))
                .matches("VALUES (1, ARRAY[1]), (2, null)");

        // PRECEDING to PRECEDING: start after end, regular, start clamped, start equal to end
        assertThat(assertions.query("SELECT a, array_agg(a) OVER(ORDER BY a ROWS BETWEEN start_offset PRECEDING AND end_offset PRECEDING) " +
                "FROM (VALUES (1, 0, 1), (2, 1, 0), (3, 5, 2), (4, 2, 2)) t(a, start_offset, end_offset)"))
                .matches("VALUES (2, ARRAY[1, 2]), (1, null), (3, ARRAY[1]), (4, ARRAY[2])");

        // FOLLOWING to FOLLOWING: start after end, regular, end clamped
        assertThat(assertions.query("SELECT a, array_agg(a) OVER(ORDER BY a ROWS BETWEEN start_offset FOLLOWING AND end_offset FOLLOWING) " +
                "FROM (VALUES (1, 1, 0), (2, 0, 1), (3, 0, 10)) t(a, start_offset, end_offset)"))
                .matches("VALUES (2, ARRAY[2, 3]), (1, null), (3, ARRAY[3])");

        // UNBOUNDED PRECEDING to PRECEDING: offset before partition start, regular, offset equal to row position
        assertThat(assertions.query("SELECT a, array_agg(a) OVER(ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND end_offset PRECEDING) " +
                "FROM (VALUES (1, 5), (2, 0), (3, 2)) t(a, end_offset)"))
                .matches("VALUES (2, ARRAY[1, 2]), (1, null), (3, ARRAY[1])");

        // FOLLOWING to UNBOUNDED FOLLOWING: offset past partition end, regular, offset equal to remaining rows
        assertThat(assertions.query("SELECT a, array_agg(a) OVER(ORDER BY a ROWS BETWEEN start_offset FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES (1, 5), (2, 0), (3, 0)) t(a, start_offset)"))
                .matches("VALUES (2, ARRAY[2, 3]), (1, null), (3, ARRAY[3])");

        // UNBOUNDED PRECEDING to PRECEDING: offsets read from the current partition, regardless of partition order
        assertThat(assertions.query("SELECT p, a, array_agg(a) OVER(PARTITION BY p ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND end_offset PRECEDING) " +
                "FROM (VALUES (1, 1, 1), (1, 2, 0), (2, 10, 0), (2, 20, 2), (2, 30, 0)) t(p, a, end_offset)"))
                .matches("VALUES (1, 2, ARRAY[1, 2]), (1, 1, null), (2, 10, ARRAY[10]), (2, 20, null), (2, 30, ARRAY[10, 20, 30])");

        // FOLLOWING to UNBOUNDED FOLLOWING: offsets read from the current partition, regardless of partition order
        assertThat(assertions.query("SELECT p, a, array_agg(a) OVER(PARTITION BY p ORDER BY a ROWS BETWEEN start_offset FOLLOWING AND UNBOUNDED FOLLOWING) " +
                "FROM (VALUES (1, 1, 2), (1, 2, 0), (2, 10, 0), (2, 20, 2), (2, 30, 0)) t(p, a, start_offset)"))
                .matches("VALUES (1, 2, ARRAY[2]), (1, 1, null), (2, 10, ARRAY[10, 20, 30]), (2, 20, null), (2, 30, ARRAY[30])");
    }
}
