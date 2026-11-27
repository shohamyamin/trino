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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.isMergeCrossJoinedGlobalAggregations;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrUtils.combineDisjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.globalAggregation;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.Patterns.join;

/**
 * Merges global aggregations over the same table that are cross joined with each other,
 * e.g. uncorrelated scalar subqueries, into one aggregation that reads the table once.
 * <p>
 * Transforms
 * <pre>
 *   - CrossJoin
 *     - Aggregation (global): f(x)
 *       - Filter (p1)
 *         - TableScan (t)
 *     - Aggregation (global): g(y)
 *       - Filter (p2)
 *         - TableScan (t)
 * </pre>
 * to
 * <pre>
 *   - Aggregation (global): f(x) with mask m1, g(y) with mask m2
 *     - Project (m1 := coalesce(p1, false), m2 := coalesce(p2, false))
 *       - Filter (p1 OR p2)
 *         - TableScan (t)
 * </pre>
 * Projections of each aggregation input are evaluated only for the rows of their own filter.
 */
public class MergeCrossJoinedGlobalAggregations
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(MergeCrossJoinedGlobalAggregations::isCrossJoin);

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMergeCrossJoinedGlobalAggregations(session);
    }

    private static boolean isCrossJoin(JoinNode join)
    {
        return join.getType() == INNER &&
                join.getCriteria().isEmpty() &&
                join.getFilter().isEmpty() &&
                join.getDistributionType().isEmpty() &&
                join.getDynamicFilters().isEmpty();
    }

    @Override
    public Result apply(JoinNode join, Captures captures, Context context)
    {
        List<PlanNode> leaves = new ArrayList<>();
        collectCrossJoinLeaves(join, context.getLookup(), leaves);

        // group mergeable aggregations by table, keeping the order of the first member
        Map<TableKey, List<Member>> groups = new LinkedHashMap<>();
        List<Object> order = new ArrayList<>();
        for (PlanNode leaf : leaves) {
            Optional<Member> member = toMember(leaf, context.getLookup());
            if (member.isEmpty()) {
                order.add(leaf);
                continue;
            }
            TableKey key = new TableKey(member.get().scan().getTable(), member.get().scan().isUpdateTarget(), member.get().scan().getUseConnectorNodePartitioning());
            List<Member> group = groups.get(key);
            if (group == null) {
                group = new ArrayList<>();
                groups.put(key, group);
                order.add(key);
            }
            group.add(member.get());
        }
        if (groups.values().stream().allMatch(group -> group.size() < 2)) {
            return Result.empty();
        }

        PlanNode result = null;
        for (Object item : order) {
            PlanNode next;
            if (item instanceof TableKey key) {
                List<Member> group = groups.get(key);
                next = group.size() == 1 ? group.getFirst().leaf() : merge(group, context);
            }
            else {
                next = (PlanNode) item;
            }
            result = result == null ? next : crossJoin(result, next, context);
        }

        return Result.ofPlanNode(new ProjectNode(context.getIdAllocator().getNextId(), result, Assignments.identity(join.getOutputSymbols())));
    }

    private static void collectCrossJoinLeaves(PlanNode node, Lookup lookup, List<PlanNode> leaves)
    {
        PlanNode resolved = lookup.resolve(node);
        if (resolved instanceof JoinNode join && isCrossJoin(join)) {
            collectCrossJoinLeaves(join.getLeft(), lookup, leaves);
            collectCrossJoinLeaves(join.getRight(), lookup, leaves);
            return;
        }
        leaves.add(resolved);
    }

    private static PlanNode crossJoin(PlanNode left, PlanNode right, Context context)
    {
        return new JoinNode(
                context.getIdAllocator().getNextId(),
                INNER,
                left,
                right,
                ImmutableList.of(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    /**
     * A global aggregation over a single table: [Project] - Aggregation - [Project] - [Filter] - TableScan
     */
    private record Member(
            PlanNode leaf,
            Optional<ProjectNode> outputProject,
            AggregationNode aggregation,
            Optional<ProjectNode> inputProject,
            Optional<FilterNode> filter,
            TableScanNode scan) {}

    private record TableKey(TableHandle table, boolean updateTarget, Optional<Boolean> useConnectorNodePartitioning) {}

    private static Optional<Member> toMember(PlanNode leaf, Lookup lookup)
    {
        PlanNode node = leaf;
        Optional<ProjectNode> outputProject = Optional.empty();
        if (node instanceof ProjectNode project) {
            if (!project.getAssignments().expressions().stream().allMatch(expression -> isDeterministic(expression))) {
                return Optional.empty();
            }
            outputProject = Optional.of(project);
            node = lookup.resolve(project.getSource());
        }
        if (!(node instanceof AggregationNode aggregation) ||
                aggregation.getStep() != SINGLE ||
                !aggregation.hasSingleGlobalAggregation() ||
                aggregation.getGroupIdSymbol().isPresent() ||
                aggregation.getAggregations().isEmpty() ||
                aggregation.getAggregations().values().stream().anyMatch(function -> function.getMask().isPresent() || function.getFilter().isPresent())) {
            return Optional.empty();
        }
        node = lookup.resolve(aggregation.getSource());
        Optional<ProjectNode> inputProject = Optional.empty();
        if (node instanceof ProjectNode project) {
            if (!project.getAssignments().expressions().stream().allMatch(expression -> isDeterministic(expression))) {
                return Optional.empty();
            }
            inputProject = Optional.of(project);
            node = lookup.resolve(project.getSource());
        }
        Optional<FilterNode> filter = Optional.empty();
        if (node instanceof FilterNode filterNode) {
            if (!isDeterministic(filterNode.getPredicate())) {
                return Optional.empty();
            }
            filter = Optional.of(filterNode);
            node = lookup.resolve(filterNode.getSource());
        }
        if (!(node instanceof TableScanNode scan)) {
            return Optional.empty();
        }
        return Optional.of(new Member(leaf, outputProject, aggregation, inputProject, filter, scan));
    }

    private static PlanNode merge(List<Member> members, Context context)
    {
        // one symbol per column of the table
        Map<ColumnHandle, Symbol> columns = new LinkedHashMap<>();
        List<Map<Symbol, Symbol>> mappings = new ArrayList<>();
        for (Member member : members) {
            Map<Symbol, Symbol> mapping = new HashMap<>();
            for (Map.Entry<Symbol, ColumnHandle> entry : member.scan().getAssignments().entrySet()) {
                Symbol column = columns.computeIfAbsent(entry.getValue(), _ -> entry.getKey());
                mapping.put(entry.getKey(), column);
            }
            mappings.add(mapping);
        }

        ImmutableList.Builder<Symbol> scanOutputs = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> scanAssignments = ImmutableMap.builder();
        columns.forEach((handle, symbol) -> {
            scanOutputs.add(symbol);
            scanAssignments.put(symbol, handle);
        });
        TableScanNode first = members.getFirst().scan();
        PlanNode source = new TableScanNode(
                context.getIdAllocator().getNextId(),
                first.getTable(),
                scanOutputs.build(),
                scanAssignments.buildOrThrow(),
                first.getEnforcedConstraint(),
                Optional.empty(),
                first.isUpdateTarget(),
                first.getUseConnectorNodePartitioning());

        List<Expression> predicates = new ArrayList<>();
        for (int i = 0; i < members.size(); i++) {
            SymbolMapper mapper = SymbolMapper.symbolMapper(mappings.get(i));
            predicates.add(members.get(i).filter().map(filter -> mapper.map(filter.getPredicate())).orElse(TRUE));
        }
        if (!predicates.contains(TRUE)) {
            source = new FilterNode(context.getIdAllocator().getNextId(), source, combineDisjuncts(predicates));
        }

        // masks are computed first, as projected aggregation inputs are evaluated only for the rows of their mask
        Assignments.Builder masks = Assignments.builder();
        masks.putIdentities(source.getOutputSymbols());
        Map<Symbol, Expression> inputProjections = new LinkedHashMap<>();
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        Assignments.Builder outputs = Assignments.builder();
        for (int i = 0; i < members.size(); i++) {
            Member member = members.get(i);
            Map<Symbol, Symbol> mapping = mappings.get(i);

            Symbol mask = context.getSymbolAllocator().newSymbol("mask", BOOLEAN);
            Expression predicate = predicates.get(i);
            masks.put(mask, predicate.equals(TRUE) ? TRUE : new Coalesce(predicate, FALSE));

            if (member.inputProject().isPresent()) {
                SymbolMapper mapper = SymbolMapper.symbolMapper(mapping);
                for (Map.Entry<Symbol, Expression> entry : member.inputProject().get().getAssignments().assignments().entrySet()) {
                    Expression expression = mapper.map(entry.getValue());
                    if (expression instanceof Reference reference) {
                        mapping.put(entry.getKey(), Symbol.from(reference));
                    }
                    else {
                        // evaluate only for the rows of this aggregation, as other rows may fail the expression
                        inputProjections.put(entry.getKey(), new Case(ImmutableList.of(new WhenClause(mask.toSymbolReference(), expression)), new Constant(entry.getKey().type(), null)));
                    }
                }
            }

            SymbolMapper mapper = SymbolMapper.symbolMapper(mapping);
            for (Map.Entry<Symbol, Aggregation> entry : member.aggregation().getAggregations().entrySet()) {
                Aggregation aggregation = mapper.map(entry.getValue());
                aggregations.put(entry.getKey(), new Aggregation(
                        aggregation.getResolvedFunction(),
                        aggregation.getArguments(),
                        aggregation.isDistinct(),
                        Optional.empty(),
                        aggregation.getOrderingScheme(),
                        Optional.of(mask)));
            }

            if (member.outputProject().isPresent()) {
                outputs.putAll(member.outputProject().get().getAssignments());
            }
            else {
                outputs.putIdentities(member.aggregation().getOutputSymbols());
            }
        }

        source = new ProjectNode(context.getIdAllocator().getNextId(), source, masks.build());
        if (!inputProjections.isEmpty()) {
            source = new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    source,
                    Assignments.builder()
                            .putIdentities(source.getOutputSymbols())
                            .putAll(inputProjections)
                            .build());
        }

        AggregationNode aggregation = AggregationNode.builderFrom(members.getFirst().aggregation())
                .setId(context.getIdAllocator().getNextId())
                .setSource(source)
                .setAggregations(aggregations.buildOrThrow())
                .setGroupingSets(globalAggregation())
                .setPreGroupedSymbols(ImmutableList.of())
                .build();
        return new ProjectNode(context.getIdAllocator().getNextId(), aggregation, outputs.build());
    }
}
