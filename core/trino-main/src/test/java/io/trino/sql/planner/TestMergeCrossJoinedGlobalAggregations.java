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
package io.trino.sql.planner;

import io.trino.Session;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.TableScanNode;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.MERGE_CROSS_JOINED_GLOBAL_AGGREGATIONS;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMergeCrossJoinedGlobalAggregations
        extends BasePlanTest
{
    @Test
    public void testMergeScalarSubqueries()
    {
        assertTableScans(
                """
                SELECT
                    (SELECT count(*) FROM orders WHERE orderstatus = 'F'),
                    (SELECT sum(totalprice) FROM orders WHERE orderstatus = 'O'),
                    (SELECT avg(totalprice) FROM orders)
                """,
                1,
                3);

        // scalar subqueries over another table are not merged with them
        assertTableScans(
                """
                SELECT
                    (SELECT count(*) FROM orders WHERE orderstatus = 'F'),
                    (SELECT max(name) FROM nation),
                    (SELECT sum(totalprice) FROM orders WHERE orderstatus = 'O')
                """,
                2,
                3);

        // subqueries cross joined with another relation
        assertTableScans(
                """
                SELECT name, CASE WHEN (SELECT count(*) FROM orders WHERE totalprice > 1000) > 10 THEN (SELECT avg(totalprice) FROM orders WHERE orderpriority = '1-URGENT') END
                FROM nation
                WHERE regionkey = 1
                """,
                2,
                3);
    }

    @Test
    public void testDoesNotMergeGroupedAggregations()
    {
        assertTableScans(
                """
                SELECT *
                FROM (SELECT orderstatus, count(*) FROM orders GROUP BY orderstatus)
                CROSS JOIN (SELECT sum(totalprice) FROM orders)
                """,
                2,
                2);
    }

    private void assertTableScans(@Language("SQL") String sql, int merged, int notMerged)
    {
        assertThat(countTableScans(sql, true)).isEqualTo(merged);
        assertThat(countTableScans(sql, false)).isEqualTo(notMerged);
    }

    private int countTableScans(@Language("SQL") String sql, boolean merge)
    {
        Session session = Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(MERGE_CROSS_JOINED_GLOBAL_AGGREGATIONS, Boolean.toString(merge))
                .build();
        Plan plan = getPlanTester().inTransaction(session, transactionSession ->
                getPlanTester().createPlan(transactionSession, sql, getPlanTester().getPlanOptimizers(false), OPTIMIZED_AND_VALIDATED, NOOP, createPlanOptimizersStatsCollector()));
        return PlanNodeSearcher.searchFrom(plan.getRoot())
                .whereIsInstanceOfAny(TableScanNode.class)
                .findAll()
                .size();
    }
}
