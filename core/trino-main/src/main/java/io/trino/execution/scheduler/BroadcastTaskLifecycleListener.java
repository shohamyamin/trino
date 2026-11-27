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
import io.trino.execution.RemoteTask;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Notifies all consumers of a stage whose output is shared by several consumer stages.
 */
class BroadcastTaskLifecycleListener
        implements TaskLifecycleListener
{
    private final List<TaskLifecycleListener> listeners;

    BroadcastTaskLifecycleListener(List<TaskLifecycleListener> listeners)
    {
        this.listeners = ImmutableList.copyOf(requireNonNull(listeners, "listeners is null"));
    }

    @Override
    public void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
    {
        listeners.forEach(listener -> listener.taskCreated(fragmentId, task));
    }

    @Override
    public void noMoreTasks(PlanFragmentId fragmentId)
    {
        listeners.forEach(listener -> listener.noMoreTasks(fragmentId));
    }
}
