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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Remote sources reading the output of each fragment. A fragment shared by several consumers,
 * e.g. a common table expression, has one consumer per remote source reading it. Consumer {@code c}
 * of a fragment with {@code n} consumers reads partition {@code p} from output buffer {@code p * n + c}.
 */
public final class ExchangeConsumers
{
    public static final ExchangeConsumers EMPTY = new ExchangeConsumers(ImmutableMap.of());

    private final Map<PlanFragmentId, List<PlanNodeId>> consumers;

    private ExchangeConsumers(Map<PlanFragmentId, List<PlanNodeId>> consumers)
    {
        this.consumers = ImmutableMap.copyOf(requireNonNull(consumers, "consumers is null"));
    }

    /**
     * @param fragments consuming fragments in a deterministic order
     */
    public static ExchangeConsumers create(Collection<PlanFragment> fragments)
    {
        Map<PlanFragmentId, List<PlanNodeId>> consumers = new LinkedHashMap<>();
        for (PlanFragment fragment : fragments) {
            for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
                for (PlanFragmentId sourceFragmentId : remoteSource.getSourceFragmentIds()) {
                    consumers.computeIfAbsent(sourceFragmentId, _ -> new ArrayList<>()).add(remoteSource.getId());
                }
            }
        }
        return new ExchangeConsumers(consumers.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue()))));
    }

    public int getConsumerCount(PlanFragmentId fragmentId)
    {
        List<PlanNodeId> fragmentConsumers = consumers.get(fragmentId);
        return fragmentConsumers == null ? 1 : fragmentConsumers.size();
    }

    public boolean isShared(PlanFragmentId fragmentId)
    {
        return getConsumerCount(fragmentId) > 1;
    }

    public OutputBufferId getOutputBufferId(PlanFragmentId fragmentId, PlanNodeId remoteSourceId, int partition)
    {
        List<PlanNodeId> fragmentConsumers = consumers.get(fragmentId);
        if (fragmentConsumers == null) {
            return new OutputBufferId(partition);
        }
        int consumer = fragmentConsumers.indexOf(remoteSourceId);
        checkArgument(consumer >= 0, "%s is not a consumer of fragment %s", remoteSourceId, fragmentId);
        return new OutputBufferId(partition * fragmentConsumers.size() + consumer);
    }
}
