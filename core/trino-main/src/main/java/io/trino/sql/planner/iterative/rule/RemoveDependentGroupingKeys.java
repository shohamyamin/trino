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
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.matching.Capture.newCapture;
import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Removes grouping keys that are determined by the other grouping keys, and computes them after the
 * aggregation instead:
 * <pre> {@code
 * - Aggregation[keys = [x, y, c]]
 *   - Project: x = x, y = x - 1, c = 'constant'
 * }
 * </pre>
 * into:
 * <pre> {@code
 * - Project: x = x, y = x - 1, c = 'constant'
 *   - Aggregation[keys = [x]]
 *     - Project: x = x
 * }
 * </pre>
 * A key is determined by the other keys when it is a constant, or a deterministic expression over
 * grouping keys that are plain column references. Values that are equal as grouping keys must be
 * indistinguishable for this to hold, so the referenced keys are restricted to types without distinct
 * representations of equal values (unlike {@code double}, with {@code 0.0} and {@code -0.0}, or
 * {@code timestamp with time zone}, with different zones of the same instant).
 * <p>
 * A key expression that may fail is only removed when it is an addition or subtraction of an integer
 * constant. The referenced key is then checked for the values for which the expression overflows,
 * and for those it evaluates the original expression, so that the same rows fail as before.
 */
public class RemoveDependentGroupingKeys
        implements Rule<AggregationNode>
{
    private static final CatalogSchemaFunctionName ADD = builtinFunctionName(OperatorType.ADD);
    private static final CatalogSchemaFunctionName SUBTRACT = builtinFunctionName(OperatorType.SUBTRACT);

    private static final Capture<ProjectNode> PROJECT_CAPTURE = newCapture();
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getStep() == SINGLE
                    && aggregation.getGroupingSetCount() == 1
                    && aggregation.getGroupIdSymbol().isEmpty()
                    && aggregation.getGroupingKeys().size() > 1)
            .with(source().matching(project().capturedAs(PROJECT_CAPTURE)));

    private final PlannerContext plannerContext;

    public RemoveDependentGroupingKeys(PlannerContext plannerContext)
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
        Assignments assignments = projectNode.getAssignments();

        // grouping keys that are plain references to a source symbol, by that symbol
        Map<Symbol, Symbol> baseKeys = new HashMap<>();
        for (Symbol key : aggregationNode.getGroupingKeys()) {
            if (assignments.get(key) instanceof Reference reference && isGroupingIdentity(reference.type())) {
                baseKeys.putIfAbsent(Symbol.from(reference), key);
            }
        }
        Set<Symbol> baseKeyOutputs = ImmutableSet.copyOf(baseKeys.values());

        Map<Symbol, Expression> dependentKeys = new LinkedHashMap<>();
        Map<Symbol, OverflowGuard> guards = new LinkedHashMap<>();
        for (Symbol key : aggregationNode.getGroupingKeys()) {
            Expression expression = assignments.get(key);
            if (baseKeyOutputs.contains(key) || expression == null || !isDeterministic(expression)) {
                continue;
            }
            Set<Symbol> references = SymbolsExtractor.extractUnique(expression);
            if (!baseKeys.keySet().containsAll(references)) {
                continue;
            }
            if (mayFail(plannerContext, getCharVarcharCoercion(context.getSession()), expression)) {
                Optional<Offset> offset = extractOffset(expression);
                if (offset.isEmpty() || !baseKeys.containsKey(offset.get().value())) {
                    continue;
                }
                guards.computeIfAbsent(offset.get().value(), _ -> new OverflowGuard()).add(offset.get());
            }
            dependentKeys.put(key, inlineSymbols(symbol -> baseKeys.get(symbol).toSymbolReference(), expression));
        }

        List<Symbol> remainingKeys = aggregationNode.getGroupingKeys().stream()
                .filter(key -> !dependentKeys.containsKey(key))
                .collect(toImmutableList());
        if (dependentKeys.isEmpty() || remainingKeys.isEmpty()) {
            return Result.empty();
        }

        Assignments.Builder sourceAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            Symbol symbol = assignment.getKey();
            Expression expression = assignment.getValue();
            if (baseKeyOutputs.contains(symbol) && guards.containsKey(Symbol.from(expression))) {
                expression = guards.get(Symbol.from(expression)).guard(expression, context);
            }
            sourceAssignments.put(symbol, expression);
        }

        AggregationNode newAggregation = AggregationNode.builderFrom(aggregationNode)
                .setSource(new ProjectNode(projectNode.getId(), projectNode.getSource(), sourceAssignments.build()))
                .setGroupingSets(singleGroupingSet(remainingKeys))
                .setPreGroupedSymbols(aggregationNode.getPreGroupedSymbols().stream()
                        .filter(symbol -> !dependentKeys.containsKey(symbol))
                        .collect(toImmutableList()))
                .build();

        Assignments.Builder outputAssignments = Assignments.builder();
        for (Symbol output : aggregationNode.getOutputSymbols()) {
            outputAssignments.put(output, dependentKeys.getOrDefault(output, output.toSymbolReference()));
        }
        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), newAggregation, outputAssignments.build()));
    }

    /**
     * Whether grouping values that are equal are also indistinguishable by any expression.
     */
    private static boolean isGroupingIdentity(Type type)
    {
        return type instanceof BigintType
                || type instanceof IntegerType
                || type instanceof SmallintType
                || type instanceof TinyintType
                || type instanceof BooleanType
                || type instanceof DateType
                || type instanceof DecimalType
                || type instanceof TimestampType
                || type instanceof VarcharType
                || type instanceof VarbinaryType;
    }

    private static Optional<Offset> extractOffset(Expression expression)
    {
        Type type = expression.type();
        if (!type.equals(BIGINT) && !type.equals(INTEGER) && !type.equals(SMALLINT) && !type.equals(TINYINT)) {
            return Optional.empty();
        }
        if (!(expression instanceof Call(ResolvedFunction operator, List<Expression> arguments)) || arguments.size() != 2) {
            return Optional.empty();
        }
        Expression left = arguments.get(0);
        Expression right = arguments.get(1);
        if (operator.name().equals(ADD) && left instanceof Reference reference && reference.type().equals(type) && right instanceof Constant(Type _, Long constant)) {
            return Optional.of(new Offset(Symbol.from(reference), constant, expression));
        }
        if (operator.name().equals(ADD) && right instanceof Reference reference && reference.type().equals(type) && left instanceof Constant(Type _, Long constant)) {
            return Optional.of(new Offset(Symbol.from(reference), constant, expression));
        }
        if (operator.name().equals(SUBTRACT) && left instanceof Reference reference && reference.type().equals(type) && right instanceof Constant(Type _, Long constant) && constant != Long.MIN_VALUE) {
            return Optional.of(new Offset(Symbol.from(reference), -constant, expression));
        }
        return Optional.empty();
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
        if (type.equals(BIGINT)) {
            return Long.MAX_VALUE;
        }
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    /**
     * {@code expression} is {@code value + offset}.
     */
    private record Offset(Symbol value, long offset, Expression expression)
    {
        private Offset
        {
            requireNonNull(value, "value is null");
            requireNonNull(expression, "expression is null");
        }
    }

    private class OverflowGuard
    {
        private Offset largest;
        private Offset smallest;

        public void add(Offset offset)
        {
            if (largest == null || offset.offset() > largest.offset()) {
                largest = offset;
            }
            if (smallest == null || offset.offset() < smallest.offset()) {
                smallest = offset;
            }
        }

        /**
         * Returns {@code value}, unless one of the additions overflows for it. Then it evaluates the
         * addition with the largest (or smallest) offset, which overflows as well and fails with the
         * error of the original expression.
         */
        public Expression guard(Expression value, Context context)
        {
            Type type = value.type();
            long maxValue = maxValue(type);
            long minValue = -maxValue - 1;
            ImmutableList.Builder<WhenClause> clauses = ImmutableList.builder();
            if (largest.offset() > 0) {
                clauses.add(new WhenClause(
                        comparison(plannerContext.getMetadata(), getCharVarcharCoercion(context.getSession()), GREATER_THAN, value, new Constant(type, maxValue - largest.offset())),
                        largest.expression()));
            }
            if (smallest.offset() < 0) {
                clauses.add(new WhenClause(
                        comparison(plannerContext.getMetadata(), getCharVarcharCoercion(context.getSession()), LESS_THAN, value, new Constant(type, minValue - smallest.offset())),
                        smallest.expression()));
            }
            List<WhenClause> whenClauses = clauses.build();
            if (whenClauses.isEmpty()) {
                return value;
            }
            return new Case(whenClauses, value);
        }
    }
}
