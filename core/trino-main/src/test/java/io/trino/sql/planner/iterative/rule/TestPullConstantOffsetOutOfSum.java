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
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.AggregationFunction;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.trino.sql.planner.assertions.PlanMatchPattern.aggregationFunction;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPullConstantOffsetOutOfSum
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction ADD_SMALLINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(SMALLINT, SMALLINT));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

    @Test
    public void testRewrite()
    {
        Reference x = new Reference(INTEGER, "x");
        Expression plusOne = new Call(ADD_INTEGER, ImmutableList.of(x, new Constant(INTEGER, 1L)));
        Expression plusTwo = new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 2L), x));
        Expression minusThree = new Call(SUBTRACT_INTEGER, ImmutableList.of(x, new Constant(INTEGER, 3L)));
        tester().assertThat(new PullConstantOffsetOutOfSum(tester().getPlannerContext()))
                .on(p -> {
                    Symbol key = p.symbol("key", BIGINT);
                    Symbol xSymbol = p.symbol("x", INTEGER);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(key)
                            .addAggregation(p.symbol("s0", BIGINT), sum("v0"), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("s1", BIGINT), sum("v1"), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("s2", BIGINT), sum("v2"), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("s3", BIGINT), sum("v3"), ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(key)
                                            .put(p.symbol("v0", BIGINT), new Cast(x, BIGINT))
                                            .put(p.symbol("v1", BIGINT), new Cast(plusOne, BIGINT))
                                            .put(p.symbol("v2", BIGINT), new Cast(plusTwo, BIGINT))
                                            .put(p.symbol("v3", BIGINT), new Cast(minusThree, BIGINT))
                                            .build(),
                                    p.values(key, xSymbol))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "key", expression(new Reference(BIGINT, "key")),
                                "s0", expression(new Reference(BIGINT, "sum")),
                                "s1", expression(shifted(1)),
                                "s2", expression(shifted(2)),
                                "s3", expression(shifted(-3))),
                        aggregation(
                                singleGroupingSet("key"),
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("guarded")),
                                        "count", aggregationFunction("count", ImmutableList.of("guarded"))),
                                project(
                                        ImmutableMap.of("guarded", expression(new Cast(
                                                new Case(
                                                        ImmutableList.of(
                                                                new WhenClause(comparison(GREATER_THAN, x, new Constant(INTEGER, (long) Integer.MAX_VALUE - 2)), plusTwo),
                                                                new WhenClause(comparison(LESS_THAN, x, new Constant(INTEGER, (long) Integer.MIN_VALUE + 3)), minusThree)),
                                                        x),
                                                BIGINT))),
                                        values("key", "x")))));
    }

    @Test
    public void testRewriteSmallint()
    {
        Reference x = new Reference(SMALLINT, "x");
        Expression plusOne = new Call(ADD_SMALLINT, ImmutableList.of(x, new Constant(SMALLINT, 1L)));
        Expression plusFive = new Call(ADD_SMALLINT, ImmutableList.of(x, new Constant(SMALLINT, 5L)));
        tester().assertThat(new PullConstantOffsetOutOfSum(tester().getPlannerContext()))
                .on(p -> {
                    Symbol xSymbol = p.symbol("x", SMALLINT);
                    return p.aggregation(builder -> builder
                            .globalGrouping()
                            .addAggregation(p.symbol("s1", BIGINT), sum("v1"), ImmutableList.of(BIGINT))
                            .addAggregation(p.symbol("s5", BIGINT), sum("v5"), ImmutableList.of(BIGINT))
                            .source(p.project(
                                    Assignments.builder()
                                            .put(p.symbol("v1", BIGINT), new Cast(plusOne, BIGINT))
                                            .put(p.symbol("v5", BIGINT), new Cast(plusFive, BIGINT))
                                            .build(),
                                    p.values(xSymbol))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "s1", expression(shifted(1)),
                                "s5", expression(shifted(5))),
                        aggregation(
                                ImmutableMap.of(
                                        "sum", aggregationFunction("sum", ImmutableList.of("guarded")),
                                        "count", aggregationFunction("count", ImmutableList.of("guarded"))),
                                project(
                                        ImmutableMap.of("guarded", expression(new Cast(
                                                new Case(
                                                        ImmutableList.of(new WhenClause(comparison(GREATER_THAN, x, new Constant(SMALLINT, (long) Short.MAX_VALUE - 5)), plusFive)),
                                                        x),
                                                BIGINT))),
                                        values("x")))));
    }

    @Test
    public void testDoesNotFireForSingleSum()
    {
        assertDoesNotFire(INTEGER, BIGINT, new Call(ADD_INTEGER, ImmutableList.of(new Reference(INTEGER, "x"), new Constant(INTEGER, 1L))));
    }

    @Test
    public void testDoesNotFireWithoutOffsets()
    {
        assertDoesNotFire(INTEGER, BIGINT, new Reference(INTEGER, "x"), new Reference(INTEGER, "x"));
    }

    @Test
    public void testDoesNotFireForBigint()
    {
        // the sum of the values may overflow when the sum of the shifted values does not
        Reference x = new Reference(BIGINT, "x");
        tester().assertThat(new PullConstantOffsetOutOfSum(tester().getPlannerContext()))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("s0", BIGINT), sum("x"), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("s1", BIGINT), sum("v1"), ImmutableList.of(BIGINT))
                        .source(p.project(
                                Assignments.builder()
                                        .putIdentity(p.symbol("x", BIGINT))
                                        .put(p.symbol("v1", BIGINT), new Call(ADD_BIGINT, ImmutableList.of(x, new Constant(BIGINT, 1L))))
                                        .build(),
                                p.values(p.symbol("x", BIGINT))))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinct()
    {
        Reference x = new Reference(INTEGER, "x");
        tester().assertThat(new PullConstantOffsetOutOfSum(tester().getPlannerContext()))
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("s0", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "v0"))), ImmutableList.of(BIGINT))
                        .addAggregation(p.symbol("s1", BIGINT), PlanBuilder.aggregation("sum", true, ImmutableList.of(new Reference(BIGINT, "v1"))), ImmutableList.of(BIGINT))
                        .source(p.project(
                                Assignments.builder()
                                        .put(p.symbol("v0", BIGINT), new Cast(x, BIGINT))
                                        .put(p.symbol("v1", BIGINT), new Cast(new Call(ADD_INTEGER, ImmutableList.of(x, new Constant(INTEGER, 1L))), BIGINT))
                                        .build(),
                                p.values(p.symbol("x", INTEGER))))))
                .doesNotFire();
    }

    private void assertDoesNotFire(Type valueType, Type castType, Expression... inputs)
    {
        tester().assertThat(new PullConstantOffsetOutOfSum(tester().getPlannerContext()))
                .on(p -> p.aggregation(builder -> {
                    Assignments.Builder assignments = Assignments.builder();
                    for (int i = 0; i < inputs.length; i++) {
                        assignments.put(p.symbol("v" + i, castType), new Cast(inputs[i], castType));
                        builder.addAggregation(p.symbol("s" + i, BIGINT), sum("v" + i), ImmutableList.of(BIGINT));
                    }
                    builder.globalGrouping()
                            .source(p.project(assignments.build(), p.values(p.symbol("x", valueType))));
                }))
                .doesNotFire();
    }

    private static AggregationFunction sum(String input)
    {
        return PlanBuilder.aggregation("sum", List.of(new Reference(BIGINT, input)));
    }

    private static Expression shifted(long offset)
    {
        return new Call(ADD_BIGINT, ImmutableList.of(
                new Reference(BIGINT, "sum"),
                new Call(MULTIPLY_BIGINT, ImmutableList.of(new Constant(BIGINT, offset), new Reference(BIGINT, "count")))));
    }
}
