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
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.GroupIdNode;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.preferPartialAggregation;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.groupId;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Double.isNaN;

/**
 * Pre-aggregates rows on all grouping columns before {@link GroupIdNode} multiplies them.
 * <p>
 * Transforms
 * <pre>
 *   - Partial Aggregation (keys: grouping set symbols, group id)
 *     - GroupId
 *       - source
 * </pre>
 * to
 * <pre>
 *   - Intermediate Aggregation (keys: grouping set symbols, group id)
 *     - GroupId (passing through intermediate states)
 *       - Partial Aggregation (keys: all grouping columns)
 *         - source
 * </pre>
 * <p>
 * Rows that agree on all grouping columns fall into the same group in every grouping set, so their
 * partial states can be combined before the rows are copied once per grouping set. GroupId then
 * copies (usually far fewer) pre-aggregated rows, and the pre-aggregation can be pushed further
 * down, e.g. through joins. If the pre-aggregation does not reduce rows, adaptive partial aggregation
 * turns it off.
 */
public class PushPartialAggregationThroughGroupId
        implements Rule<AggregationNode>
{
    private static final Capture<GroupIdNode> GROUP_ID = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(aggregation -> aggregation.getStep() == PARTIAL
                    && !aggregation.getGroupingKeys().isEmpty()
                    && !aggregation.getAggregations().isEmpty())
            .with(source().matching(groupId().capturedAs(GROUP_ID)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return preferPartialAggregation(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        GroupIdNode groupId = captures.get(GROUP_ID);

        if (groupId.getGroupingSets().size() < 2) {
            return Result.empty();
        }

        Set<Symbol> passThroughSymbols = ImmutableSet.copyOf(groupId.getAggregationArguments());
        Set<Symbol> groupingSetSymbols = groupId.getDistinctGroupingSetSymbols();

        // Grouping keys of the pre-aggregation: all grouping columns, in terms of GroupId input symbols,
        // plus grouping keys that are passed through GroupId unchanged
        Set<Symbol> preAggregationKeys = new LinkedHashSet<>(groupId.getGroupingColumns().values());
        List<Symbol> passThroughKeys = aggregation.getGroupingKeys().stream()
                .filter(passThroughSymbols::contains)
                .collect(toImmutableList());
        for (Symbol key : aggregation.getGroupingKeys()) {
            if (!key.equals(groupId.getGroupIdSymbol()) && !groupingSetSymbols.contains(key) && !passThroughSymbols.contains(key)) {
                return Result.empty();
            }
        }
        preAggregationKeys.addAll(passThroughKeys);
        if (preAggregationKeys.isEmpty()) {
            return Result.empty();
        }

        // Aggregation inputs must not depend on GroupId output (grouping set symbols nulled per grouping set, or group id)
        for (Aggregation aggregationFunction : aggregation.getAggregations().values()) {
            if (aggregationFunction.isDistinct() || aggregationFunction.getOrderingScheme().isPresent()) {
                return Result.empty();
            }
            if (!passThroughSymbols.containsAll(SymbolsExtractor.extractUnique(aggregationFunction))) {
                return Result.empty();
            }
        }

        if (isIneffective(preAggregationKeys, groupId, context)) {
            return Result.empty();
        }

        ImmutableMap.Builder<Symbol, Aggregation> preAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Aggregation> intermediateAggregations = ImmutableMap.builder();
        ImmutableList.Builder<Symbol> intermediateSymbols = ImmutableList.builder();
        for (Map.Entry<Symbol, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            Aggregation original = entry.getValue();
            // output of the original partial aggregation already has the intermediate type
            Symbol intermediateSymbol = context.getSymbolAllocator().newSymbol(entry.getKey());
            intermediateSymbols.add(intermediateSymbol);
            preAggregations.put(intermediateSymbol, original);
            intermediateAggregations.put(
                    entry.getKey(),
                    new Aggregation(
                            original.getResolvedFunction(),
                            ImmutableList.<Expression>builder()
                                    .add(intermediateSymbol.toSymbolReference())
                                    .addAll(original.getArguments().stream()
                                            .filter(Lambda.class::isInstance)
                                            .collect(toImmutableList()))
                                    .build(),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
        }

        AggregationNode preAggregation = AggregationNode.builderFrom(aggregation)
                .setId(context.getIdAllocator().getNextId())
                .setSource(groupId.getSource())
                .setAggregations(preAggregations.buildOrThrow())
                .setGroupingSets(singleGroupingSet(ImmutableList.copyOf(preAggregationKeys)))
                .setPreGroupedSymbols(ImmutableList.of())
                .setGroupIdSymbol(Optional.empty())
                .setStep(PARTIAL)
                .build();

        GroupIdNode newGroupId = new GroupIdNode(
                groupId.getId(),
                preAggregation,
                groupId.getGroupingSets(),
                groupId.getGroupingColumns(),
                ImmutableList.<Symbol>builder()
                        .addAll(passThroughKeys)
                        .addAll(intermediateSymbols.build())
                        .build(),
                groupId.getGroupIdSymbol());

        return Result.ofPlanNode(AggregationNode.builderFrom(aggregation)
                .setSource(newGroupId)
                .setAggregations(intermediateAggregations.buildOrThrow())
                .setPreGroupedSymbols(ImmutableList.of())
                .setStep(INTERMEDIATE)
                .build());
    }

    private static boolean isIneffective(Set<Symbol> preAggregationKeys, GroupIdNode groupId, Context context)
    {
        // Skip when statistics show that some grouping column is close to unique, as then
        // the pre-aggregation is unlikely to reduce rows. Unknown statistics do not prevent it, as
        // adaptive partial aggregation turns off an ineffective pre-aggregation.
        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(groupId.getSource());
        double rowCount = sourceStats.getOutputRowCount();
        if (isNaN(rowCount)) {
            return false;
        }
        for (Symbol key : preAggregationKeys) {
            double ndv = sourceStats.getSymbolStatistics(key).getDistinctValuesCount();
            if (!isNaN(ndv) && ndv * 2 > rowCount) {
                return true;
            }
        }
        return false;
    }
}
