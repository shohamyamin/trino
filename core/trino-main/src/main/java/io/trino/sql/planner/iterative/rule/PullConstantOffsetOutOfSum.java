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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.IrExpressions.call;
import static io.trino.sql.ir.IrExpressions.cast;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

/**
 * Computes sums of an integer value shifted by different constants from a single sum and count
 * of the value:
 * <pre> {@code
 * - Aggregation[key]: s0 = sum(CAST(x AS bigint)), s1 = sum(CAST(x + 1 AS bigint)), s2 = sum(CAST(x - 2 AS bigint))
 *   - Project: x + 1, x - 2, ...
 * }
 * </pre>
 * into:
 * <pre> {@code
 * - Project: s0 = s, s1 = s + 1 * c, s2 = s + (-2) * c
 *   - Aggregation[key]: s = sum(guarded), c = count(guarded)
 *     - Project: guarded = CAST(CASE WHEN x > max - 1 THEN x + 1 WHEN x < min + 2 THEN x - 2 ELSE x END AS bigint)
 * }
 * </pre>
 * The rewrite is exact:
 * <ul>
 *     <li>The shifted value is null exactly when {@code x} is null, so {@code count(x)} is the number of
 *     summed values, and the sum stays null when there are none.
 *     <li>The original addition fails for a row when it overflows the type of {@code x}. Such a row is
 *     caught by the guard, which evaluates the original addition with the largest (or smallest) offset,
 *     so the query fails with the same error the original plan raises for that row.
 *     <li>{@code x} is at most 32 bits wide, so neither the original nor the rewritten sums can overflow
 *     {@code bigint} for fewer than 2^32 rows per group.
 * </ul>
 */
public class PullConstantOffsetOutOfSum
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName SUM = builtinFunctionName("sum");
    private static final CatalogSchemaFunctionName ADD = builtinFunctionName(OperatorType.ADD);
    private static final CatalogSchemaFunctionName SUBTRACT = builtinFunctionName(OperatorType.SUBTRACT);

    private static final Capture<ProjectNode> PROJECT_CAPTURE = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getStep() == SINGLE)
            .with(source().matching(project().capturedAs(PROJECT_CAPTURE)));

    private final PlannerContext plannerContext;

    public PullConstantOffsetOutOfSum(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        ProjectNode projectNode = captures.get(PROJECT_CAPTURE);

        ImmutableListMultimap.Builder<SumInput, OffsetSum> candidatesBuilder = ImmutableListMultimap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            extractOffsetSum(entry.getKey(), entry.getValue(), projectNode)
                    .ifPresent(offsetSum -> candidatesBuilder.put(offsetSum.input(), offsetSum));
        }
        ListMultimap<SumInput, OffsetSum> candidates = candidatesBuilder.build();

        Assignments.Builder sourceAssignments = Assignments.builder().putAll(projectNode.getAssignments());
        Map<Symbol, Aggregation> aggregations = new LinkedHashMap<>(aggregationNode.getAggregations());
        Assignments.Builder outputAssignments = Assignments.builder();
        ImmutableMap.Builder<Symbol, Expression> rewrittenOutputs = ImmutableMap.builder();
        boolean rewritten = false;
        ResolvedFunction count = plannerContext.getMetadata().resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "count", ImmutableList.of(BIGINT));
        ResolvedFunction add = plannerContext.getMetadata().resolveOperator(getCharVarcharCoercion(context.getSession()), OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
        ResolvedFunction multiply = plannerContext.getMetadata().resolveOperator(getCharVarcharCoercion(context.getSession()), OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));

        for (Map.Entry<SumInput, Collection<OffsetSum>> group : candidates.asMap().entrySet()) {
            List<OffsetSum> sums = ImmutableList.copyOf(group.getValue());
            // a single sum is not cheaper as a sum and a count, and sums without offsets are already as cheap as possible
            if (sums.size() < 2 || sums.stream().allMatch(sum -> sum.offset() == 0)) {
                continue;
            }
            rewritten = true;
            SumInput input = group.getKey();

            Expression guarded = cast(
                    plannerContext.getTypeManager(),
                    getCharVarcharCoercion(context.getSession()),
                    overflowGuard(input.value(), sums, context),
                    BIGINT);
            Symbol guardedSymbol = context.getSymbolAllocator().newSymbol(guarded);
            sourceAssignments.put(guardedSymbol, guarded);

            OffsetSum first = sums.getFirst();
            Symbol sumSymbol = context.getSymbolAllocator().newSymbol(first.symbol());
            Symbol countSymbol = context.getSymbolAllocator().newSymbol("count", BIGINT);
            aggregations.put(sumSymbol, new Aggregation(first.function(), ImmutableList.of(guardedSymbol.toSymbolReference()), false, Optional.empty(), Optional.empty(), input.mask()));
            aggregations.put(countSymbol, new Aggregation(count, ImmutableList.of(guardedSymbol.toSymbolReference()), false, Optional.empty(), Optional.empty(), input.mask()));

            for (OffsetSum sum : sums) {
                aggregations.remove(sum.symbol());
                Expression value = sumSymbol.toSymbolReference();
                if (sum.offset() != 0) {
                    value = call(add, value, call(multiply, new Constant(BIGINT, sum.offset()), countSymbol.toSymbolReference()));
                }
                rewrittenOutputs.put(sum.symbol(), value);
            }
        }

        if (!rewritten) {
            return Result.empty();
        }

        Map<Symbol, Expression> rewrittenOutputMap = rewrittenOutputs.buildOrThrow();
        for (Symbol output : aggregationNode.getOutputSymbols()) {
            outputAssignments.put(output, rewrittenOutputMap.getOrDefault(output, output.toSymbolReference()));
        }

        ProjectNode newSource = new ProjectNode(projectNode.getId(), projectNode.getSource(), sourceAssignments.build());
        AggregationNode newAggregation = AggregationNode.builderFrom(aggregationNode)
                .setSource(newSource)
                .setAggregations(aggregations)
                .build();
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newAggregation, outputAssignments.build()));
    }

    /**
     * Returns {@code value}, unless one of the additions in {@code sums} overflows for it. Then it
     * evaluates the addition with the largest (or smallest) offset, which overflows as well, and fails
     * with the error of the original addition.
     */
    private Expression overflowGuard(Expression value, List<OffsetSum> sums, Context context)
    {
        Type type = value.type();
        long maxValue = maxValue(type);
        long minValue = -maxValue - 1;
        OffsetSum largest = sums.stream().max(comparingLong(OffsetSum::offset)).orElseThrow();
        OffsetSum smallest = sums.stream().min(comparingLong(OffsetSum::offset)).orElseThrow();

        ImmutableList.Builder<WhenClause> clauses = ImmutableList.builder();
        if (largest.offset() > 0) {
            clauses.add(new WhenClause(
                    comparison(plannerContext.getMetadata(), getCharVarcharCoercion(context.getSession()), GREATER_THAN, value, new Constant(type, maxValue - largest.offset())),
                    largest.shiftedValue()));
        }
        if (smallest.offset() < 0) {
            clauses.add(new WhenClause(
                    comparison(plannerContext.getMetadata(), getCharVarcharCoercion(context.getSession()), LESS_THAN, value, new Constant(type, minValue - smallest.offset())),
                    smallest.shiftedValue()));
        }
        return new Case(clauses.build(), value);
    }

    private static Optional<OffsetSum> extractOffsetSum(Symbol symbol, Aggregation aggregation, ProjectNode projectNode)
    {
        ResolvedFunction function = aggregation.getResolvedFunction();
        if (!function.name().equals(SUM)
                || aggregation.getArguments().size() != 1
                || !(aggregation.getArguments().getFirst() instanceof Reference argument)
                || !argument.type().equals(BIGINT)
                || !function.signature().getReturnType().equals(BIGINT)
                || aggregation.isDistinct()
                || aggregation.getFilter().isPresent()
                || aggregation.getOrderingScheme().isPresent()) {
            return Optional.empty();
        }

        Expression projection = projectNode.getAssignments().get(Symbol.from(argument));
        if (!(projection instanceof Cast(Expression shiftedValue, Type castType, Cast.Kind _)) || !castType.equals(BIGINT)) {
            return Optional.empty();
        }
        Type type = shiftedValue.type();
        if (!type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT)) {
            return Optional.empty();
        }

        Expression value = shiftedValue;
        long offset = 0;
        if (shiftedValue instanceof Call(ResolvedFunction operator, List<Expression> arguments) && arguments.size() == 2) {
            Expression left = arguments.get(0);
            Expression right = arguments.get(1);
            if (operator.name().equals(ADD) && right instanceof Constant(Type _, Long constant) && left.type().equals(type)) {
                value = left;
                offset = constant;
            }
            else if (operator.name().equals(ADD) && left instanceof Constant(Type _, Long constant) && right.type().equals(type)) {
                value = right;
                offset = constant;
            }
            else if (operator.name().equals(SUBTRACT) && right instanceof Constant(Type _, Long constant) && left.type().equals(type)) {
                value = left;
                offset = -constant;
            }
        }

        if (value instanceof Constant || !isDeterministic(value)) {
            return Optional.empty();
        }
        return Optional.of(new OffsetSum(symbol, function, new SumInput(value, aggregation.getMask()), offset, shiftedValue));
    }

    private static long maxValue(Type type)
    {
        if (type.equals(TINYINT)) {
            return Byte.MAX_VALUE;
        }
        if (type.equals(SMALLINT)) {
            return Short.MAX_VALUE;
        }
        if (type.equals(INTEGER)) {
            return Integer.MAX_VALUE;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private record SumInput(Expression value, Optional<Symbol> mask)
    {
        private SumInput
        {
            requireNonNull(value, "value is null");
            requireNonNull(mask, "mask is null");
        }
    }

    /**
     * {@code sum(CAST(shiftedValue AS bigint))}, where {@code shiftedValue} is {@code input.value + offset}.
     */
    private record OffsetSum(Symbol symbol, ResolvedFunction function, SumInput input, long offset, Expression shiftedValue)
    {
        private OffsetSum
        {
            requireNonNull(symbol, "symbol is null");
            requireNonNull(function, "function is null");
            requireNonNull(input, "input is null");
            requireNonNull(shiftedValue, "shiftedValue is null");
        }
    }
}
