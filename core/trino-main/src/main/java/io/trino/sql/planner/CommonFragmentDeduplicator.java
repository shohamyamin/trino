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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.sql.DynamicFilters.extractDynamicFilters;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * Finds fragment subtrees that compute the same result, typically copies of an inlined
 * common table expression, and keeps a single copy whose output is read by all consumers.
 * <p>
 * Two fragment subtrees are equivalent when they are structurally identical modulo symbol names,
 * plan node ids and dynamic filter ids, are deterministic, and produce the same output partitioning.
 * Dynamic filters that are consumed in a shared subtree but produced outside of it are removed from
 * the kept copy, as they depend on one particular consumer.
 */
public final class CommonFragmentDeduplicator
{
    // dynamic filters are removed only from scans this many times smaller than the largest scan of a shared subtree
    private static final double SMALL_SCAN_FRACTION = 10;

    private final Map<PlanFragmentId, SubPlan> subPlans = new HashMap<>();
    private final Map<PlanFragmentId, PlanFragmentId> parents = new HashMap<>();
    private final Map<PlanFragmentId, Set<DynamicFilterId>> producedDynamicFilters = new HashMap<>();

    // duplicate fragment -> kept fragment
    private final Map<PlanFragmentId, PlanFragmentId> replacements = new HashMap<>();
    // kept fragment -> fragments consuming it
    private final Map<PlanFragmentId, List<PlanFragmentId>> consumers = new HashMap<>();
    // kept fragment -> dynamic filters produced outside of it that every copy consumes in the same place
    private final Map<PlanFragmentId, Set<DynamicFilterId>> commonDynamicFilters = new HashMap<>();

    private CommonFragmentDeduplicator() {}

    public static SubPlan deduplicate(SubPlan root)
    {
        return new CommonFragmentDeduplicator().process(root);
    }

    private SubPlan process(SubPlan root)
    {
        index(root, Optional.empty());

        List<SubPlan> candidates = new ArrayList<>();
        Queue<SubPlan> queue = new ArrayDeque<>();
        queue.add(root);
        while (!queue.isEmpty()) {
            SubPlan current = queue.remove();
            PlanFragmentId currentId = current.getFragment().getId();
            boolean eligible = isEligible(current);
            Optional<SubPlan> equivalent = Optional.empty();
            if (eligible) {
                for (SubPlan candidate : candidates) {
                    if (!isCompatible(current, candidate)) {
                        continue;
                    }
                    Matcher matcher = new Matcher(candidate, current);
                    if (matcher.matches()) {
                        equivalent = Optional.of(candidate);
                        commonDynamicFilters.merge(
                                candidate.getFragment().getId(),
                                matcher.getCommonExternalDynamicFilters(),
                                (existing, added) -> existing.stream().filter(added::contains).collect(toImmutableSet()));
                        break;
                    }
                }
            }
            if (equivalent.isPresent()) {
                PlanFragmentId keptId = equivalent.get().getFragment().getId();
                replacements.put(currentId, keptId);
                consumers.get(keptId).add(parents.get(currentId));
                continue;
            }
            if (eligible) {
                candidates.add(current);
                consumers.computeIfAbsent(currentId, _ -> new ArrayList<>()).add(parents.get(currentId));
            }
            queue.addAll(current.getChildren());
        }

        if (replacements.isEmpty()) {
            return root;
        }

        // dynamic filters allowed in each fragment of a shared subtree
        Map<PlanFragmentId, Set<DynamicFilterId>> allowedDynamicFilters = new HashMap<>();
        for (Map.Entry<PlanFragmentId, List<PlanFragmentId>> entry : consumers.entrySet()) {
            if (entry.getValue().size() > 1) {
                Set<DynamicFilterId> allowed = ImmutableSet.<DynamicFilterId>builder()
                        .addAll(producedDynamicFilters.get(entry.getKey()))
                        .addAll(commonDynamicFilters.getOrDefault(entry.getKey(), ImmutableSet.of()))
                        .build();
                restrictDynamicFilters(subPlans.get(entry.getKey()), allowed, allowedDynamicFilters);
            }
        }

        return rebuild(root, allowedDynamicFilters, new HashMap<>());
    }

    private void index(SubPlan subPlan, Optional<PlanFragmentId> parent)
    {
        PlanFragmentId id = subPlan.getFragment().getId();
        subPlans.put(id, subPlan);
        parent.ifPresent(parentId -> parents.put(id, parentId));
        Set<DynamicFilterId> produced = new HashSet<>(producedDynamicFilters(subPlan.getFragment().getRoot()));
        for (SubPlan child : subPlan.getChildren()) {
            index(child, Optional.of(id));
            produced.addAll(producedDynamicFilters.get(child.getFragment().getId()));
        }
        producedDynamicFilters.put(id, produced);
    }

    private static double maxScanRows(SubPlan subPlan)
    {
        double max = 0;
        for (PlanFragment fragment : subPlan.getAllFragments()) {
            for (PlanNode node : PlanNodeSearcher.searchFrom(fragment.getRoot()).whereIsInstanceOfAny(TableScanNode.class).findAll()) {
                double rows = estimatedRows(fragment, node);
                if (isNaN(rows)) {
                    return NaN;
                }
                max = Math.max(max, rows);
            }
        }
        return max;
    }

    private static double estimatedRows(PlanFragment fragment, PlanNode node)
    {
        PlanNodeStatsEstimate estimate = fragment.getStatsAndCosts().getStats().get(node.getId());
        return estimate == null ? NaN : estimate.getOutputRowCount();
    }

    private static Set<DynamicFilterId> producedDynamicFilters(PlanNode root)
    {
        ImmutableSet.Builder<DynamicFilterId> result = ImmutableSet.builder();
        for (PlanNode node : PlanNodeSearcher.searchFrom(root).findAll()) {
            if (node instanceof JoinNode join) {
                result.addAll(join.getDynamicFilters().keySet());
            }
            else if (node instanceof SemiJoinNode semiJoin) {
                semiJoin.getDynamicFilterId().ifPresent(result::add);
            }
            else if (node instanceof DynamicFilterSourceNode source) {
                result.addAll(source.getDynamicFilters().keySet());
            }
        }
        return result.build();
    }

    private boolean isEligible(SubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        PlanFragmentId parentId = parents.get(fragment.getId());
        if (parentId == null) {
            return false;
        }
        PartitioningHandle outputHandle = fragment.getOutputPartitioningScheme().getPartitioning().getHandle();
        if (!outputHandle.equals(FIXED_HASH_DISTRIBUTION) && !outputHandle.equals(FIXED_BROADCAST_DISTRIBUTION) && !outputHandle.equals(SINGLE_DISTRIBUTION)) {
            return false;
        }
        if (fragment.getOutputPartitioningScheme().isReplicateNullsAndAny()) {
            return false;
        }
        PlanFragment parent = subPlans.get(parentId).getFragment();
        if (parent.getPartitioning().isCoordinatorOnly()) {
            return false;
        }
        RemoteSourceNode remoteSource = getConsumingRemoteSource(parent, fragment.getId());
        return remoteSource.getOrderingScheme().isEmpty();
    }

    private boolean isCompatible(SubPlan subPlan, SubPlan candidate)
    {
        PlanFragmentId id = subPlan.getFragment().getId();
        PlanFragmentId candidateId = candidate.getFragment().getId();
        if (!subPlans.get(id).getFragment().getPartitioning().equals(candidate.getFragment().getPartitioning())) {
            return false;
        }
        PlanFragment parent = subPlans.get(parents.get(id)).getFragment();
        if (getConsumingRemoteSource(parent, id).getSourceFragmentIds().stream()
                .map(fragmentId -> replacements.getOrDefault(fragmentId, fragmentId))
                .anyMatch(candidateId::equals)) {
            // the same remote source would read the shared fragment twice
            return false;
        }
        for (PlanFragmentId consumerId : consumers.get(candidateId)) {
            PlanFragment consumer = subPlans.get(consumerId).getFragment();
            // consumers must agree on how the shared output is partitioned
            if (!consumer.getPartitioning().equals(parent.getPartitioning()) || !consumer.getPartitionCount().equals(parent.getPartitionCount())) {
                return false;
            }
        }
        return true;
    }

    private boolean isReadOnProbeSide(SubPlan subPlan)
    {
        PlanFragmentId id = subPlan.getFragment().getId();
        PlanFragment parent = subPlans.get(parents.get(id)).getFragment();
        return isOnProbeSide(parent.getRoot(), getConsumingRemoteSource(parent, id).getId());
    }

    private static boolean isOnProbeSide(PlanNode node, PlanNodeId target)
    {
        if (node.getId().equals(target)) {
            return true;
        }
        List<PlanNode> sources = node.getSources();
        for (int i = 0; i < sources.size(); i++) {
            if (!PlanNodeSearcher.searchFrom(sources.get(i)).where(child -> child.getId().equals(target)).matches()) {
                continue;
            }
            boolean probe = switch (node) {
                case JoinNode join -> sources.get(i) == join.getLeft();
                case SemiJoinNode semiJoin -> sources.get(i) == semiJoin.getSource();
                default -> sources.size() == 1 || node instanceof ExchangeNode || node instanceof UnionNode;
            };
            return probe && isOnProbeSide(sources.get(i), target);
        }
        return false;
    }

    private static RemoteSourceNode getConsumingRemoteSource(PlanFragment parent, PlanFragmentId childId)
    {
        return parent.getRemoteSourceNodes().stream()
                .filter(node -> node.getSourceFragmentIds().contains(childId))
                .collect(onlyElement());
    }

    private void restrictDynamicFilters(SubPlan subPlan, Set<DynamicFilterId> produced, Map<PlanFragmentId, Set<DynamicFilterId>> allowed)
    {
        PlanFragmentId id = subPlan.getFragment().getId();
        allowed.merge(id, produced, (existing, added) -> existing.stream().filter(added::contains).collect(toImmutableSet()));
        for (SubPlan child : subPlan.getChildren()) {
            restrictDynamicFilters(child, produced, allowed);
        }
    }

    private SubPlan rebuild(SubPlan subPlan, Map<PlanFragmentId, Set<DynamicFilterId>> allowedDynamicFilters, Map<PlanFragmentId, SubPlan> rebuilt)
    {
        PlanFragmentId id = subPlan.getFragment().getId();
        SubPlan existing = rebuilt.get(id);
        if (existing != null) {
            return existing;
        }

        Map<PlanFragmentId, SubPlan> children = new LinkedHashMap<>();
        for (SubPlan child : subPlan.getChildren()) {
            PlanFragmentId childId = replacements.getOrDefault(child.getFragment().getId(), child.getFragment().getId());
            children.computeIfAbsent(childId, _ -> rebuild(subPlans.get(childId), allowedDynamicFilters, rebuilt));
        }

        PlanFragment fragment = subPlan.getFragment();
        PlanNode root = SimplePlanRewriter.rewriteWith(new Rewriter(Optional.ofNullable(allowedDynamicFilters.get(id))), fragment.getRoot());
        if (root != fragment.getRoot()) {
            fragment = new PlanFragment(
                    fragment.getId(),
                    root,
                    fragment.getSymbols(),
                    fragment.getPartitioning(),
                    fragment.getPartitionCount(),
                    fragment.getPartitionedSources(),
                    fragment.getOutputPartitioningScheme(),
                    fragment.getOutputSkewedBucketCount(),
                    fragment.getStatsAndCosts(),
                    fragment.getActiveCatalogs(),
                    fragment.getLanguageFunctions());
        }
        SubPlan result = new SubPlan(fragment, ImmutableList.copyOf(children.values()));
        rebuilt.put(id, result);
        return result;
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final Optional<Set<DynamicFilterId>> allowedDynamicFilters;

        private Rewriter(Optional<Set<DynamicFilterId>> allowedDynamicFilters)
        {
            this.allowedDynamicFilters = requireNonNull(allowedDynamicFilters, "allowedDynamicFilters is null");
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            List<PlanFragmentId> sourceFragmentIds = node.getSourceFragmentIds().stream()
                    .map(id -> replacements.getOrDefault(id, id))
                    .collect(toImmutableList());
            if (sourceFragmentIds.equals(node.getSourceFragmentIds())) {
                return node;
            }
            return new RemoteSourceNode(node.getId(), sourceFragmentIds, node.getOutputSymbols(), node.getOrderingScheme(), node.getExchangeType(), node.getRetryPolicy());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            if (allowedDynamicFilters.isEmpty()) {
                return source == node.getSource() ? node : new FilterNode(node.getId(), source, node.getPredicate());
            }
            List<Expression> conjuncts = extractConjuncts(node.getPredicate());
            List<Expression> retained = conjuncts.stream()
                    .filter(conjunct -> DynamicFilters.getDescriptor(conjunct)
                            .map(descriptor -> allowedDynamicFilters.get().contains(descriptor.getId()))
                            .orElse(true))
                    .collect(toImmutableList());
            if (retained.size() == conjuncts.size()) {
                return source == node.getSource() ? node : new FilterNode(node.getId(), source, node.getPredicate());
            }
            if (retained.isEmpty()) {
                return source;
            }
            return new FilterNode(node.getId(), source, combineConjuncts(retained));
        }
    }

    /**
     * Checks whether two fragment subtrees compute the same output.
     */
    private class Matcher
    {
        private final SubPlan left;
        private final SubPlan right;
        private final Set<DynamicFilterId> leftProduced;
        private final Set<DynamicFilterId> rightProduced;
        private final Map<DynamicFilterId, DynamicFilterId> producerPairs = new HashMap<>();
        private final Map<DynamicFilterId, DynamicFilterId> consumerPairs = new HashMap<>();
        private final Map<PlanNodeId, PlanNodeId> nodePairs = new HashMap<>();
        private final Set<DynamicFilterId> commonExternal = new HashSet<>();
        private final double maxScanRows;
        private PlanFragment currentLeftFragment;
        // whether the left or right copy loses a dynamic filter on a scan that is not small
        private boolean leftLosesFilter;
        private boolean rightLosesFilter;

        private Matcher(SubPlan left, SubPlan right)
        {
            this.left = left;
            this.right = right;
            this.leftProduced = producedDynamicFilters.get(left.getFragment().getId());
            this.rightProduced = producedDynamicFilters.get(right.getFragment().getId());
            this.maxScanRows = maxScanRows(left);
        }

        Set<DynamicFilterId> getCommonExternalDynamicFilters()
        {
            return ImmutableSet.copyOf(commonExternal);
        }

        boolean matches()
        {
            if (!subPlans(left, right)) {
                return false;
            }
            // A copy may lose dynamic filters only if the other copy keeps all of its own, so that
            // the shared copy computes what the other copy computes anyway, and only if the copy is
            // read on the probe side of joins, where the additional rows do not grow hash tables or
            // weaken the dynamic filters produced from them.
            if (leftLosesFilter && (rightLosesFilter || !isReadOnProbeSide(left))) {
                return false;
            }
            if (rightLosesFilter && !isReadOnProbeSide(right)) {
                return false;
            }
            for (Map.Entry<DynamicFilterId, DynamicFilterId> entry : consumerPairs.entrySet()) {
                if (!entry.getValue().equals(producerPairs.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }

        private boolean subPlans(SubPlan left, SubPlan right)
        {
            PlanFragment leftFragment = left.getFragment();
            PlanFragment rightFragment = right.getFragment();
            if (!leftFragment.getPartitioning().equals(rightFragment.getPartitioning()) ||
                    !leftFragment.getPartitionCount().equals(rightFragment.getPartitionCount()) ||
                    !leftFragment.getOutputSkewedBucketCount().equals(rightFragment.getOutputSkewedBucketCount()) ||
                    left.getChildren().size() != right.getChildren().size()) {
                return false;
            }

            SymbolMapping mapping = new SymbolMapping();
            PlanFragment parentFragment = currentLeftFragment;
            currentLeftFragment = leftFragment;
            boolean matches = nodes(leftFragment.getRoot(), rightFragment.getRoot(), mapping);
            currentLeftFragment = parentFragment;
            if (!matches) {
                return false;
            }

            List<PlanNodeId> mappedSources = leftFragment.getPartitionedSources().stream()
                    .map(nodePairs::get)
                    .collect(toImmutableList());
            if (!mappedSources.equals(rightFragment.getPartitionedSources())) {
                return false;
            }

            return partitioningSchemes(leftFragment.getOutputPartitioningScheme(), rightFragment.getOutputPartitioningScheme(), mapping);
        }

        private boolean partitioningSchemes(PartitioningScheme left, PartitioningScheme right, SymbolMapping mapping)
        {
            return mapping.map(left.getOutputLayout()).equals(right.getOutputLayout()) &&
                    left.getPartitioning().translate(mapping::map).equals(right.getPartitioning()) &&
                    left.isReplicateNullsAndAny() == right.isReplicateNullsAndAny() &&
                    left.getBucketCount().equals(right.getBucketCount()) &&
                    left.getPartitionCount().equals(right.getPartitionCount()) &&
                    left.getBucketToPartition().isPresent() == right.getBucketToPartition().isPresent() &&
                    (left.getBucketToPartition().isEmpty() || Arrays.equals(left.getBucketToPartition().get(), right.getBucketToPartition().get()));
        }

        private boolean nodes(PlanNode left, PlanNode right, SymbolMapping mapping)
        {
            // filters on dynamic filters produced outside the subtree are removed from the kept copy,
            // unless every copy consumes the same dynamic filter in the same place
            List<FilterNode> leftFilters = new ArrayList<>();
            List<FilterNode> rightFilters = new ArrayList<>();
            left = skipExternalDynamicFilters(left, leftProduced, leftFilters);
            right = skipExternalDynamicFilters(right, rightProduced, rightFilters);
            if (left instanceof FilterNode filter) {
                leftFilters.add(filter);
            }
            if (right instanceof FilterNode filter) {
                rightFilters.add(filter);
            }
            if (!nodesWithoutExternalDynamicFilters(left, right, mapping)) {
                return false;
            }
            externalDynamicFilters(leftFilters, rightFilters, left, mapping);
            return true;
        }

        private boolean nodesWithoutExternalDynamicFilters(PlanNode left, PlanNode right, SymbolMapping mapping)
        {
            if (left.getClass() != right.getClass() || left.getSources().size() != right.getSources().size()) {
                return false;
            }
            for (int i = 0; i < left.getSources().size(); i++) {
                if (!nodes(left.getSources().get(i), right.getSources().get(i), mapping)) {
                    return false;
                }
            }
            nodePairs.put(left.getId(), right.getId());

            return switch (left) {
                case TableScanNode node -> tableScan(node, (TableScanNode) right, mapping);
                case FilterNode node -> filter(node, (FilterNode) right, mapping);
                case ProjectNode node -> project(node, (ProjectNode) right, mapping);
                case JoinNode node -> join(node, (JoinNode) right, mapping);
                case SemiJoinNode node -> semiJoin(node, (SemiJoinNode) right, mapping);
                case AggregationNode node -> aggregation(node, (AggregationNode) right, mapping);
                case ExchangeNode node -> exchange(node, (ExchangeNode) right, mapping);
                case RemoteSourceNode node -> remoteSource(node, (RemoteSourceNode) right, mapping);
                case DynamicFilterSourceNode node -> producedDynamicFilters(node.getDynamicFilters(), ((DynamicFilterSourceNode) right).getDynamicFilters(), mapping);
                case WindowNode node -> window(node, (WindowNode) right, mapping);
                case TopNRankingNode node -> topNRanking(node, (TopNRankingNode) right, mapping);
                case RowNumberNode node -> rowNumber(node, (RowNumberNode) right, mapping);
                case TopNNode node -> node.getCount() == ((TopNNode) right).getCount() &&
                        node.getStep() == ((TopNNode) right).getStep() &&
                        mapping.mapper().map(node.getOrderingScheme()).equals(((TopNNode) right).getOrderingScheme());
                case LimitNode node -> limit(node, (LimitNode) right, mapping);
                case SortNode node -> node.isPartial() == ((SortNode) right).isPartial() &&
                        mapping.mapper().map(node.getOrderingScheme()).equals(((SortNode) right).getOrderingScheme());
                case MarkDistinctNode node -> mapping.map(node.getDistinctSymbols()).equals(((MarkDistinctNode) right).getDistinctSymbols()) &&
                        mapping.bind(node.getMarkerSymbol(), ((MarkDistinctNode) right).getMarkerSymbol());
                case ValuesNode node -> values(node, (ValuesNode) right, mapping);
                case EnforceSingleRowNode _ -> true;
                case GroupIdNode node -> groupId(node, (GroupIdNode) right, mapping);
                default -> false;
            };
        }

        private static PlanNode skipExternalDynamicFilters(PlanNode node, Set<DynamicFilterId> produced, List<FilterNode> skipped)
        {
            while (node instanceof FilterNode filter) {
                DynamicFilters.ExtractResult result = extractDynamicFilters(filter.getPredicate());
                if (!result.staticConjuncts().isEmpty() || result.dynamicConjuncts().stream().anyMatch(descriptor -> produced.contains(descriptor.getId()))) {
                    return node;
                }
                skipped.add(filter);
                node = filter.getSource();
            }
            return node;
        }

        /**
         * Dynamic filters produced outside of the shared subtree are kept only if all copies consume them
         * in the same place. Others are removed. Removing them from scans that are not small compared to
         * the largest scan of the subtree is allowed only for one of the two copies, so that the shared
         * copy computes what one of the copies computes anyway.
         */
        private void externalDynamicFilters(List<FilterNode> leftFilters, List<FilterNode> rightFilters, PlanNode leftNode, SymbolMapping mapping)
        {
            List<DynamicFilters.Descriptor> leftExternal = externalDescriptors(leftFilters, leftProduced);
            List<DynamicFilters.Descriptor> rightExternal = new ArrayList<>(externalDescriptors(rightFilters, rightProduced));
            boolean leftRemoved = false;
            for (DynamicFilters.Descriptor descriptor : leftExternal) {
                Optional<DynamicFilters.Descriptor> same = rightExternal.stream()
                        .filter(rightDescriptor -> rightDescriptor.getId().equals(descriptor.getId()) &&
                                rightDescriptor.getOperator() == descriptor.getOperator() &&
                                rightDescriptor.isNullAllowed() == descriptor.isNullAllowed() &&
                                rightDescriptor.getInput().equals(mapping.mapper().map(descriptor.getInput())))
                        .findFirst();
                if (same.isPresent()) {
                    commonExternal.add(descriptor.getId());
                    rightExternal.remove(same.get());
                }
                else {
                    leftRemoved = true;
                }
            }
            boolean rightRemoved = !rightExternal.isEmpty();
            if ((leftRemoved || rightRemoved) && !isSmallScan(leftNode)) {
                leftLosesFilter |= leftRemoved;
                rightLosesFilter |= rightRemoved;
            }
        }

        private static List<DynamicFilters.Descriptor> externalDescriptors(List<FilterNode> filters, Set<DynamicFilterId> produced)
        {
            return filters.stream()
                    .flatMap(filter -> extractDynamicFilters(filter.getPredicate()).dynamicConjuncts().stream())
                    .filter(descriptor -> !produced.contains(descriptor.getId()))
                    .collect(toImmutableList());
        }

        private boolean isSmallScan(PlanNode node)
        {
            while (!(node instanceof TableScanNode)) {
                if (!(node instanceof FilterNode) && !(node instanceof ProjectNode)) {
                    return false;
                }
                node = node.getSources().getFirst();
            }
            double rows = estimatedRows(currentLeftFragment, node);
            return !isNaN(rows) && !isNaN(maxScanRows) && rows * SMALL_SCAN_FRACTION <= maxScanRows;
        }

        private boolean tableScan(TableScanNode left, TableScanNode right, SymbolMapping mapping)
        {
            if (!left.getTable().equals(right.getTable()) ||
                    left.isUpdateTarget() != right.isUpdateTarget() ||
                    !left.getUseConnectorNodePartitioning().equals(right.getUseConnectorNodePartitioning()) ||
                    left.getOutputSymbols().size() != right.getOutputSymbols().size()) {
                return false;
            }
            Map<ColumnHandle, Symbol> rightColumns = new HashMap<>();
            for (Symbol symbol : right.getOutputSymbols()) {
                if (rightColumns.put(right.getAssignments().get(symbol), symbol) != null) {
                    return false;
                }
            }
            for (Symbol symbol : left.getOutputSymbols()) {
                Symbol rightSymbol = rightColumns.get(left.getAssignments().get(symbol));
                if (rightSymbol == null || !mapping.bind(symbol, rightSymbol)) {
                    return false;
                }
            }
            return true;
        }

        private boolean filter(FilterNode left, FilterNode right, SymbolMapping mapping)
        {
            DynamicFilters.ExtractResult leftResult = extractDynamicFilters(left.getPredicate());
            DynamicFilters.ExtractResult rightResult = extractDynamicFilters(right.getPredicate());
            if (!leftResult.staticConjuncts().stream().allMatch(DeterminismEvaluator::isDeterministic)) {
                return false;
            }
            List<Expression> leftStatic = leftResult.staticConjuncts().stream()
                    .map(mapping.mapper()::map)
                    .collect(toImmutableList());
            if (!leftStatic.equals(rightResult.staticConjuncts())) {
                return false;
            }
            List<DynamicFilters.Descriptor> leftDynamic = leftResult.dynamicConjuncts().stream()
                    .filter(descriptor -> leftProduced.contains(descriptor.getId()))
                    .collect(toImmutableList());
            List<DynamicFilters.Descriptor> rightDynamic = rightResult.dynamicConjuncts().stream()
                    .filter(descriptor -> rightProduced.contains(descriptor.getId()))
                    .collect(toImmutableList());
            if (leftDynamic.size() != rightDynamic.size()) {
                return false;
            }
            for (int i = 0; i < leftDynamic.size(); i++) {
                DynamicFilters.Descriptor leftDescriptor = leftDynamic.get(i);
                DynamicFilters.Descriptor rightDescriptor = rightDynamic.get(i);
                if (leftDescriptor.getOperator() != rightDescriptor.getOperator() ||
                        leftDescriptor.isNullAllowed() != rightDescriptor.isNullAllowed() ||
                        !mapping.mapper().map(leftDescriptor.getInput()).equals(rightDescriptor.getInput())) {
                    return false;
                }
                DynamicFilterId previous = consumerPairs.putIfAbsent(leftDescriptor.getId(), rightDescriptor.getId());
                if (previous != null && !previous.equals(rightDescriptor.getId())) {
                    return false;
                }
            }
            return true;
        }

        private boolean project(ProjectNode left, ProjectNode right, SymbolMapping mapping)
        {
            Map<Symbol, Expression> leftAssignments = left.getAssignments().assignments();
            Map<Symbol, Expression> rightAssignments = right.getAssignments().assignments();
            if (leftAssignments.size() != rightAssignments.size()) {
                return false;
            }
            Set<Symbol> matched = new HashSet<>();
            for (Map.Entry<Symbol, Expression> entry : leftAssignments.entrySet()) {
                if (!isDeterministic(entry.getValue())) {
                    return false;
                }
                Expression expression = mapping.mapper().map(entry.getValue());
                Optional<Symbol> rightSymbol = rightAssignments.entrySet().stream()
                        .filter(rightEntry -> !matched.contains(rightEntry.getKey()) &&
                                rightEntry.getKey().type().equals(entry.getKey().type()) &&
                                rightEntry.getValue().equals(expression))
                        .map(Map.Entry::getKey)
                        .findFirst();
                if (rightSymbol.isEmpty() || !mapping.bind(entry.getKey(), rightSymbol.get())) {
                    return false;
                }
                matched.add(rightSymbol.get());
            }
            return true;
        }

        private boolean join(JoinNode left, JoinNode right, SymbolMapping mapping)
        {
            if (left.getType() != right.getType() ||
                    !left.getDistributionType().equals(right.getDistributionType()) ||
                    left.isMaySkipOutputDuplicates() != right.isMaySkipOutputDuplicates() ||
                    !left.isSpillable().equals(right.isSpillable())) {
                return false;
            }
            List<JoinNode.EquiJoinClause> criteria = left.getCriteria().stream()
                    .map(clause -> new JoinNode.EquiJoinClause(mapping.map(clause.getLeft()), mapping.map(clause.getRight())))
                    .collect(toImmutableList());
            if (!criteria.equals(right.getCriteria())) {
                return false;
            }
            if (left.getFilter().isPresent() != right.getFilter().isPresent()) {
                return false;
            }
            if (left.getFilter().isPresent() &&
                    (!isDeterministic(left.getFilter().get()) || !mapping.mapper().map(left.getFilter().get()).equals(right.getFilter().get()))) {
                return false;
            }
            if (!ImmutableSet.copyOf(mapping.map(left.getLeftOutputSymbols())).equals(ImmutableSet.copyOf(right.getLeftOutputSymbols())) ||
                    !ImmutableSet.copyOf(mapping.map(left.getRightOutputSymbols())).equals(ImmutableSet.copyOf(right.getRightOutputSymbols()))) {
                return false;
            }
            return producedDynamicFilters(left.getDynamicFilters(), right.getDynamicFilters(), mapping);
        }

        private boolean semiJoin(SemiJoinNode left, SemiJoinNode right, SymbolMapping mapping)
        {
            if (!mapping.map(left.getSourceJoinSymbol()).equals(right.getSourceJoinSymbol()) ||
                    !mapping.map(left.getFilteringSourceJoinSymbol()).equals(right.getFilteringSourceJoinSymbol()) ||
                    !left.getDistributionType().equals(right.getDistributionType()) ||
                    left.getDynamicFilterId().isPresent() != right.getDynamicFilterId().isPresent()) {
                return false;
            }
            if (left.getDynamicFilterId().isPresent() && !producerPair(left.getDynamicFilterId().get(), right.getDynamicFilterId().get())) {
                return false;
            }
            return mapping.bind(left.getSemiJoinOutput(), right.getSemiJoinOutput());
        }

        private boolean producedDynamicFilters(Map<DynamicFilterId, Symbol> left, Map<DynamicFilterId, Symbol> right, SymbolMapping mapping)
        {
            if (left.size() != right.size()) {
                return false;
            }
            for (Map.Entry<DynamicFilterId, Symbol> entry : left.entrySet()) {
                Symbol symbol = mapping.map(entry.getValue());
                List<DynamicFilterId> candidates = right.entrySet().stream()
                        .filter(rightEntry -> rightEntry.getValue().equals(symbol))
                        .map(Map.Entry::getKey)
                        .collect(toImmutableList());
                if (candidates.size() != 1 || !producerPair(entry.getKey(), candidates.get(0))) {
                    return false;
                }
            }
            return true;
        }

        private boolean producerPair(DynamicFilterId left, DynamicFilterId right)
        {
            DynamicFilterId previous = producerPairs.putIfAbsent(left, right);
            return previous == null || previous.equals(right);
        }

        private boolean aggregation(AggregationNode left, AggregationNode right, SymbolMapping mapping)
        {
            if (left.getStep() != right.getStep() ||
                    left.getGroupingSetCount() != right.getGroupingSetCount() ||
                    !left.getGlobalGroupingSets().equals(right.getGlobalGroupingSets()) ||
                    left.isInputReducingAggregation() != right.isInputReducingAggregation() ||
                    !mapping.map(left.getGroupingKeys()).equals(right.getGroupingKeys()) ||
                    !mapping.map(left.getPreGroupedSymbols()).equals(right.getPreGroupedSymbols()) ||
                    !left.getGroupIdSymbol().map(mapping::map).equals(right.getGroupIdSymbol()) ||
                    left.getAggregations().size() != right.getAggregations().size()) {
                return false;
            }
            Set<Symbol> matched = new HashSet<>();
            for (Map.Entry<Symbol, Aggregation> entry : left.getAggregations().entrySet()) {
                Aggregation aggregation = mapping.mapper().map(entry.getValue());
                Optional<Symbol> rightSymbol = right.getAggregations().entrySet().stream()
                        .filter(rightEntry -> !matched.contains(rightEntry.getKey()) && rightEntry.getValue().equals(aggregation))
                        .map(Map.Entry::getKey)
                        .findFirst();
                if (rightSymbol.isEmpty() || !mapping.bind(entry.getKey(), rightSymbol.get())) {
                    return false;
                }
                matched.add(rightSymbol.get());
            }
            return true;
        }

        private boolean exchange(ExchangeNode left, ExchangeNode right, SymbolMapping mapping)
        {
            if (left.getScope() != LOCAL || right.getScope() != LOCAL || left.getType() != right.getType()) {
                return false;
            }
            for (int i = 0; i < left.getInputs().size(); i++) {
                if (!mapping.map(left.getInputs().get(i)).equals(right.getInputs().get(i))) {
                    return false;
                }
            }
            if (!mapping.bind(left.getOutputSymbols(), right.getOutputSymbols())) {
                return false;
            }
            return left.getOrderingScheme().map(mapping.mapper()::map).equals(right.getOrderingScheme()) &&
                    partitioningSchemes(left.getPartitioningScheme(), right.getPartitioningScheme(), mapping);
        }

        private boolean remoteSource(RemoteSourceNode left, RemoteSourceNode right, SymbolMapping mapping)
        {
            if (left.getExchangeType() != right.getExchangeType() ||
                    left.getRetryPolicy() != right.getRetryPolicy() ||
                    left.getSourceFragmentIds().size() != right.getSourceFragmentIds().size()) {
                return false;
            }
            for (int i = 0; i < left.getSourceFragmentIds().size(); i++) {
                if (!subPlans(subPlans.get(left.getSourceFragmentIds().get(i)), subPlans.get(right.getSourceFragmentIds().get(i)))) {
                    return false;
                }
            }
            return mapping.bind(left.getOutputSymbols(), right.getOutputSymbols()) &&
                    left.getOrderingScheme().map(mapping.mapper()::map).equals(right.getOrderingScheme());
        }

        private boolean window(WindowNode left, WindowNode right, SymbolMapping mapping)
        {
            if (left.getWindowFunctions().size() != right.getWindowFunctions().size() ||
                    left.getPreSortedOrderPrefix() != right.getPreSortedOrderPrefix()) {
                return false;
            }
            List<Symbol> leftOutputs = ImmutableList.copyOf(left.getWindowFunctions().keySet());
            List<Symbol> rightOutputs = ImmutableList.copyOf(right.getWindowFunctions().keySet());
            if (!mapping.bind(leftOutputs, rightOutputs)) {
                return false;
            }
            WindowNode mapped = mapping.mapper().map(left, left.getSource());
            return mapped.getSpecification().equals(right.getSpecification()) &&
                    mapped.getPrePartitionedInputs().equals(right.getPrePartitionedInputs()) &&
                    mapped.getPreSortedOrderPrefix() == right.getPreSortedOrderPrefix() &&
                    mapped.getWindowFunctions().equals(right.getWindowFunctions()) &&
                    left.getWindowFunctions().values().stream()
                            .flatMap(function -> function.getArguments().stream())
                            .allMatch(DeterminismEvaluator::isDeterministic);
        }

        private boolean topNRanking(TopNRankingNode left, TopNRankingNode right, SymbolMapping mapping)
        {
            return left.getRankingType() == right.getRankingType() &&
                    left.getMaxRankingPerPartition() == right.getMaxRankingPerPartition() &&
                    left.isPartial() == right.isPartial() &&
                    mapping.mapper().mapAndDistinct(left.getSpecification()).equals(right.getSpecification()) &&
                    mapping.bind(left.getRankingSymbol(), right.getRankingSymbol());
        }

        private boolean rowNumber(RowNumberNode left, RowNumberNode right, SymbolMapping mapping)
        {
            return left.isOrderSensitive() == right.isOrderSensitive() &&
                    left.getMaxRowCountPerPartition().equals(right.getMaxRowCountPerPartition()) &&
                    mapping.map(left.getPartitionBy()).equals(right.getPartitionBy()) &&
                    mapping.bind(left.getRowNumberSymbol(), right.getRowNumberSymbol());
        }

        private boolean limit(LimitNode left, LimitNode right, SymbolMapping mapping)
        {
            return left.getCount() == right.getCount() &&
                    left.isPartial() == right.isPartial() &&
                    left.getTiesResolvingScheme().map(mapping.mapper()::map).equals(right.getTiesResolvingScheme()) &&
                    mapping.map(left.getPreSortedInputs()).equals(right.getPreSortedInputs());
        }

        private boolean values(ValuesNode left, ValuesNode right, SymbolMapping mapping)
        {
            if (left.getRowCount() != right.getRowCount() || left.getRows().isPresent() != right.getRows().isPresent()) {
                return false;
            }
            if (left.getRows().isPresent()) {
                List<Expression> leftRows = left.getRows().get();
                if (!leftRows.stream().allMatch(DeterminismEvaluator::isDeterministic) ||
                        !leftRows.stream().map(mapping.mapper()::map).collect(toImmutableList()).equals(right.getRows().get())) {
                    return false;
                }
            }
            return mapping.bind(left.getOutputSymbols(), right.getOutputSymbols());
        }

        private boolean groupId(GroupIdNode left, GroupIdNode right, SymbolMapping mapping)
        {
            if (left.getGroupingColumns().size() != right.getGroupingColumns().size() ||
                    !mapping.map(left.getAggregationArguments()).equals(right.getAggregationArguments())) {
                return false;
            }
            Set<Symbol> matched = new HashSet<>();
            for (Map.Entry<Symbol, Symbol> entry : left.getGroupingColumns().entrySet()) {
                Symbol input = mapping.map(entry.getValue());
                Optional<Symbol> rightOutput = right.getGroupingColumns().entrySet().stream()
                        .filter(rightEntry -> !matched.contains(rightEntry.getKey()) && rightEntry.getValue().equals(input))
                        .map(Map.Entry::getKey)
                        .findFirst();
                if (rightOutput.isEmpty() || !mapping.bind(entry.getKey(), rightOutput.get())) {
                    return false;
                }
                matched.add(rightOutput.get());
            }
            List<List<Symbol>> groupingSets = left.getGroupingSets().stream()
                    .map(mapping::map)
                    .collect(toImmutableList());
            return groupingSets.equals(right.getGroupingSets()) &&
                    mapping.bind(left.getGroupIdSymbol(), right.getGroupIdSymbol());
        }
    }

    /**
     * One-to-one mapping between the symbols of two equivalent fragments.
     */
    private static class SymbolMapping
    {
        private final Map<Symbol, Symbol> forward = new HashMap<>();
        private final Map<Symbol, Symbol> backward = new HashMap<>();
        private final SymbolMapper mapper = new SymbolMapper(this::map);

        boolean bind(Symbol left, Symbol right)
        {
            if (!left.type().equals(right.type())) {
                return false;
            }
            Symbol existing = forward.get(left);
            if (existing != null) {
                return existing.equals(right);
            }
            if (backward.containsKey(right)) {
                return false;
            }
            forward.put(left, right);
            backward.put(right, left);
            return true;
        }

        boolean bind(List<Symbol> left, List<Symbol> right)
        {
            if (left.size() != right.size()) {
                return false;
            }
            for (int i = 0; i < left.size(); i++) {
                if (!bind(left.get(i), right.get(i))) {
                    return false;
                }
            }
            return true;
        }

        Symbol map(Symbol symbol)
        {
            Symbol mapped = forward.get(symbol);
            if (mapped != null) {
                return mapped;
            }
            // never equal to a symbol of the other fragment
            return new Symbol(symbol.type(), "$unmapped$" + symbol.name());
        }

        List<Symbol> map(List<Symbol> symbols)
        {
            return symbols.stream()
                    .map(this::map)
                    .collect(toImmutableList());
        }

        SymbolMapper mapper()
        {
            return mapper;
        }
    }
}
