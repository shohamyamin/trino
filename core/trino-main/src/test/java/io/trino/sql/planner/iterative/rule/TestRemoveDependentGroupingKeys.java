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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
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
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.aggregation;

public class TestRemoveDependentGroupingKeys
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction SUBTRACT_INTEGER = FUNCTIONS.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testRemoveConstantAndNonFailingKeys()
    {
        tester().assertThat(new RemoveDependentGroupingKeys(tester().getPlannerContext()))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol constant = p.symbol("constant", INTEGER);
                    Symbol isOne = p.symbol("is_one", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(x, constant, isOne)
                            .addAggregation(p.symbol("count", BIGINT), aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(x)
                                            .put(constant, new Constant(INTEGER, 1L))
                                            .put(isOne, comparison(EQUAL, x.toSymbolReference(), new Constant(BIGINT, 1L)))
                                            .build(),
                                    p.values(x))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "x", expression(new Reference(BIGINT, "x")),
                                "constant", expression(new Constant(INTEGER, 1L)),
                                "is_one", expression(comparison(EQUAL, new Reference(BIGINT, "x"), new Constant(BIGINT, 1L))),
                                "count", expression(new Reference(BIGINT, "count"))),
                        aggregation(
                                singleGroupingSet("x"),
                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("x", expression(new Reference(BIGINT, "x"))),
                                        values("x")))));
    }

    @Test
    public void testRemoveShiftedKeys()
    {
        Reference x = new Reference(INTEGER, "x");
        Expression minusOne = new Call(SUBTRACT_INTEGER, ImmutableList.of(x, new Constant(INTEGER, 1L)));
        Expression plusTwo = new Call(ADD_INTEGER, ImmutableList.of(new Constant(INTEGER, 2L), x));
        tester().assertThat(new RemoveDependentGroupingKeys(tester().getPlannerContext()))
                .on(p -> {
                    Symbol xSymbol = p.symbol("x", INTEGER);
                    Symbol y = p.symbol("y", INTEGER);
                    Symbol z = p.symbol("z", INTEGER);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(xSymbol, y, z)
                            .addAggregation(p.symbol("count", BIGINT), aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(xSymbol)
                                            .put(y, minusOne)
                                            .put(z, plusTwo)
                                            .build(),
                                    p.values(xSymbol))));
                })
                .matches(strictProject(
                        ImmutableMap.of(
                                "x", expression(x),
                                "y", expression(minusOne),
                                "z", expression(plusTwo),
                                "count", expression(new Reference(BIGINT, "count"))),
                        aggregation(
                                singleGroupingSet("x"),
                                ImmutableMap.of("count", aggregationFunction("count", ImmutableList.of())),
                                project(
                                        ImmutableMap.of("x", expression(new Case(
                                                ImmutableList.of(
                                                        new WhenClause(comparison(GREATER_THAN, x, new Constant(INTEGER, (long) Integer.MAX_VALUE - 2)), plusTwo),
                                                        new WhenClause(comparison(LESS_THAN, x, new Constant(INTEGER, (long) Integer.MIN_VALUE + 1)), minusOne)),
                                                x))),
                                        values("x")))));
    }

    @Test
    public void testDoesNotFireForDoubleKey()
    {
        tester().assertThat(new RemoveDependentGroupingKeys(tester().getPlannerContext()))
                .on(p -> {
                    Symbol x = p.symbol("x", DOUBLE);
                    Symbol isZero = p.symbol("is_zero", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(x, isZero)
                            .addAggregation(p.symbol("count", BIGINT), aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(x)
                                            .put(isZero, comparison(EQUAL, x.toSymbolReference(), new Constant(DOUBLE, 0.0)))
                                            .build(),
                                    p.values(x))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForOnlyConstantKeys()
    {
        tester().assertThat(new RemoveDependentGroupingKeys(tester().getPlannerContext()))
                .on(p -> {
                    Symbol first = p.symbol("first", INTEGER);
                    Symbol second = p.symbol("second", INTEGER);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(first, second)
                            .addAggregation(p.symbol("count", BIGINT), aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .put(first, new Constant(INTEGER, 1L))
                                            .put(second, new Constant(INTEGER, 2L))
                                            .build(),
                                    p.values(p.symbol("x", INTEGER)))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForExpressionOverNonKey()
    {
        tester().assertThat(new RemoveDependentGroupingKeys(tester().getPlannerContext()))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol y = p.symbol("y", BIGINT);
                    Symbol isOne = p.symbol("is_one", BOOLEAN);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(x, isOne)
                            .addAggregation(p.symbol("count", BIGINT), aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(x)
                                            .put(isOne, comparison(EQUAL, y.toSymbolReference(), new Constant(BIGINT, 1L)))
                                            .build(),
                                    p.values(x, y))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFailingExpression()
    {
        ResolvedFunction divide = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(INTEGER, INTEGER));
        tester().assertThat(new RemoveDependentGroupingKeys(tester().getPlannerContext()))
                .on(p -> {
                    Symbol x = p.symbol("x", INTEGER);
                    Symbol quotient = p.symbol("quotient", INTEGER);
                    return p.aggregation(builder -> builder
                            .singleGroupingSet(x, quotient)
                            .addAggregation(p.symbol("count", BIGINT), aggregation("count", List.of()), List.of())
                            .source(p.project(
                                    Assignments.builder()
                                            .putIdentity(x)
                                            .put(quotient, new Call(divide, ImmutableList.of(new Constant(INTEGER, 10L), x.toSymbolReference())))
                                            .build(),
                                    p.values(x))));
                })
                .doesNotFire();
    }
}
