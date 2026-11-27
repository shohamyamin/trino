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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNodeId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.groupId;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestPushPartialAggregationThroughGroupId
        extends BaseRuleTest
{
    private static final PlanNodeId SOURCE_ID = new PlanNodeId("source_id");

    @Test
    public void testPreAggregatesBelowGroupId()
    {
        tester().assertThat(new PushPartialAggregationThroughGroupId())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol x = p.symbol("x");
                    Symbol groupIdSymbol = p.symbol("group_id");
                    return p.aggregation(ab -> ab
                            .source(p.groupId(
                                    ImmutableList.of(ImmutableList.of(), ImmutableList.of(a), ImmutableList.of(a, b)),
                                    ImmutableList.of(x),
                                    groupIdSymbol,
                                    p.values(a, b, x)))
                            .addAggregation(p.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new Reference(BIGINT, "x"))), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("avg", anonymousRow(DOUBLE, BIGINT)), aggregation("avg", ImmutableList.of(new Reference(BIGINT, "x"))), ImmutableList.of(BIGINT))
                            .groupingSets(AggregationNode.groupingSets(ImmutableList.of(a, b, groupIdSymbol), 3, ImmutableSet.of(0)))
                            .step(PARTIAL));
                })
                .matches(aggregation(
                        ImmutableMap.of(
                                "sum", aggregationFunction("sum", ImmutableList.of("pre_sum")),
                                "avg", aggregationFunction("avg", ImmutableList.of("pre_avg"))),
                        node -> node.getStep() == INTERMEDIATE
                                && node.getGroupingKeys().size() == 3
                                && node.getGroupingSetCount() == 3
                                && node.getGlobalGroupingSets().equals(ImmutableSet.of(0)),
                        groupId(
                                ImmutableList.of(ImmutableList.of(), ImmutableList.of("a"), ImmutableList.of("a", "b")),
                                ImmutableList.of("pre_sum", "pre_avg"),
                                "group_id",
                                aggregation(
                                        singleGroupingSet("a", "b"),
                                        ImmutableMap.of(
                                                Optional.of("pre_sum"), aggregationFunction("sum", ImmutableList.of("x")),
                                                Optional.of("pre_avg"), aggregationFunction("avg", ImmutableList.of("x"))),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("a", "b", "x")))));
    }

    @Test
    public void testDoesNotFireForSingleGroupingSet()
    {
        tester().assertThat(new PushPartialAggregationThroughGroupId())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol x = p.symbol("x");
                    Symbol groupIdSymbol = p.symbol("group_id");
                    return p.aggregation(ab -> ab
                            .source(p.groupId(ImmutableList.of(ImmutableList.of(a)), ImmutableList.of(x), groupIdSymbol, p.values(a, x)))
                            .addAggregation(p.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new Reference(BIGINT, "x"))), ImmutableList.of(BIGINT))
                            .singleGroupingSet(a, groupIdSymbol)
                            .step(PARTIAL));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonPartialAggregation()
    {
        tester().assertThat(new PushPartialAggregationThroughGroupId())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol x = p.symbol("x");
                    Symbol groupIdSymbol = p.symbol("group_id");
                    return p.aggregation(ab -> ab
                            .source(p.groupId(ImmutableList.of(ImmutableList.of(a), ImmutableList.of(b)), ImmutableList.of(x), groupIdSymbol, p.values(a, b, x)))
                            .addAggregation(p.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new Reference(BIGINT, "x"))), ImmutableList.of(BIGINT))
                            .singleGroupingSet(a, b, groupIdSymbol)
                            .step(SINGLE));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationUsesGroupIdOutput()
    {
        // the aggregation input is a grouping set column, which GroupId nulls out in some grouping sets
        tester().assertThat(new PushPartialAggregationThroughGroupId())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol groupIdSymbol = p.symbol("group_id");
                    return p.aggregation(ab -> ab
                            .source(p.groupId(ImmutableList.of(ImmutableList.of(a), ImmutableList.of(b)), ImmutableList.of(), groupIdSymbol, p.values(a, b)))
                            .addAggregation(p.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new Reference(BIGINT, "a"))), ImmutableList.of(BIGINT))
                            .singleGroupingSet(a, b, groupIdSymbol)
                            .step(PARTIAL));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNearlyUniqueGroupingColumn()
    {
        tester().assertThat(new PushPartialAggregationThroughGroupId())
                .overrideStats(SOURCE_ID.toString(), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addSymbolStatistics(new Symbol(BIGINT, "a"), SymbolStatsEstimate.builder().setDistinctValuesCount(10).build())
                        .addSymbolStatistics(new Symbol(BIGINT, "b"), SymbolStatsEstimate.builder().setDistinctValuesCount(900).build())
                        .build())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol x = p.symbol("x");
                    Symbol groupIdSymbol = p.symbol("group_id");
                    return p.aggregation(ab -> ab
                            .source(p.groupId(ImmutableList.of(ImmutableList.of(a), ImmutableList.of(a, b)), ImmutableList.of(x), groupIdSymbol, p.values(SOURCE_ID, a, b, x)))
                            .addAggregation(p.symbol("sum", BIGINT), aggregation("sum", ImmutableList.of(new Reference(BIGINT, "x"))), ImmutableList.of(BIGINT))
                            .singleGroupingSet(a, b, groupIdSymbol)
                            .step(PARTIAL));
                })
                .doesNotFire();
    }
}
