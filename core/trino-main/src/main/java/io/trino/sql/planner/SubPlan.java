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
import com.google.common.collect.Multiset;
import com.google.errorprone.annotations.Immutable;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public class SubPlan
{
    private final PlanFragment fragment;
    private final List<SubPlan> children;

    public SubPlan(PlanFragment fragment, List<SubPlan> children)
    {
        requireNonNull(fragment, "fragment is null");
        requireNonNull(children, "children is null");

        this.fragment = fragment;
        this.children = ImmutableList.copyOf(children);
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<SubPlan> getChildren()
    {
        return children;
    }

    /**
     * Flattens the subplan and returns all PlanFragments in the tree
     */
    public List<PlanFragment> getAllFragments()
    {
        // a fragment shared by several consumers is listed once
        Map<PlanFragmentId, PlanFragment> fragments = new LinkedHashMap<>();
        collectFragments(fragments);
        return ImmutableList.copyOf(fragments.values());
    }

    private void collectFragments(Map<PlanFragmentId, PlanFragment> fragments)
    {
        if (fragments.putIfAbsent(fragment.getId(), fragment) != null) {
            return;
        }
        for (SubPlan child : children) {
            child.collectFragments(fragments);
        }
    }

    public void sanityCheck()
    {
        // a fragment may be read by several remote sources of the same fragment
        Set<PlanFragmentId> exchangeIds = fragment.getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Multiset<PlanFragmentId> childrenIds = children.stream()
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .collect(toImmutableMultiset());

        checkState(childrenIds.elementSet().size() == childrenIds.size(), "Subplan has duplicate children: %s", childrenIds);

        checkState(exchangeIds.equals(childrenIds.elementSet()), "Subplan exchange ids don't match child fragment ids (%s vs %s)", exchangeIds, childrenIds);

        for (SubPlan child : children) {
            child.sanityCheck();
        }
    }
}
