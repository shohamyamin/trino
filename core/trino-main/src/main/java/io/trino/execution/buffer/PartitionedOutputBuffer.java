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
package io.trino.execution.buffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import io.trino.execution.buffer.SerializedPageReference.PagesReleasedListener;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.plugin.base.util.Lazy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.buffer.BufferState.FLUSHING;
import static io.trino.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.PARTITIONED;
import static io.trino.execution.buffer.SerializedPageReference.dereferencePages;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputBuffer
        implements OutputBuffer
{
    private final OutputBufferStateMachine stateMachine;
    private final PipelinedOutputBuffers outputBuffers;
    private final OutputBufferMemoryManager memoryManager;
    private final PagesReleasedListener onPagesReleased;

    // indexed by output buffer id
    private final List<ClientBuffer> partitions;
    // indexed by partition, every page of a partition is delivered to all its client buffers
    private final List<List<ClientBuffer>> partitionBuffers;

    private final LongAdder totalPagesAdded = new LongAdder();
    private final LongAdder totalRowsAdded = new LongAdder();

    public PartitionedOutputBuffer(
            long taskInstanceId,
            OutputBufferStateMachine stateMachine,
            PipelinedOutputBuffers outputBuffers,
            DataSize maxBufferSize,
            Lazy<LocalMemoryContext> memoryContextSupplier,
            Executor notificationExecutor)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");

        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == PARTITIONED, "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = outputBuffers;
        this.memoryManager = new OutputBufferMemoryManager(
                maxBufferSize.toBytes(),
                requireNonNull(memoryContextSupplier, "memoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
        this.onPagesReleased = PagesReleasedListener.forOutputBufferMemoryManager(memoryManager);

        int bufferCount = outputBuffers.getBuffers().size();
        ClientBuffer[] buffers = new ClientBuffer[bufferCount];
        int partitionCount = 0;
        for (Map.Entry<OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
            int id = entry.getKey().id();
            checkArgument(id < bufferCount, "Output buffer ids must be dense: %s", outputBuffers.getBuffers());
            buffers[id] = new ClientBuffer(taskInstanceId, entry.getKey(), onPagesReleased);
            partitionCount = Math.max(partitionCount, entry.getValue() + 1);
        }
        this.partitions = ImmutableList.copyOf(buffers);
        List<ImmutableList.Builder<ClientBuffer>> partitionBuffers = new ArrayList<>();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitionBuffers.add(ImmutableList.builder());
        }
        for (Map.Entry<OutputBufferId, Integer> entry : outputBuffers.getBuffers().entrySet()) {
            partitionBuffers.get(entry.getValue()).add(buffers[entry.getKey().id()]);
        }
        this.partitionBuffers = partitionBuffers.stream()
                .map(ImmutableList.Builder::build)
                .collect(toImmutableList());
        if (outputBuffers.isNoBlockOnFull()) {
            memoryManager.setNoBlockOnFull();
        }

        stateMachine.noMoreBuffers();
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    @Override
    public OutputBufferStatus getStatus()
    {
        return OutputBufferStatus.builder(outputBuffers.getVersion())
                .setOverutilized(memoryManager.isOverutilized())
                .build();
    }

    @Override
    public boolean usesExternalStorage()
    {
        return false;
    }

    @Override
    public OutputBufferInfo getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = stateMachine.getState();

        int totalBufferedPages = 0;
        ImmutableList.Builder<PipelinedBufferInfo> infos = ImmutableList.builderWithExpectedSize(partitions.size());
        for (ClientBuffer partition : partitions) {
            PipelinedBufferInfo bufferInfo = partition.getInfo();
            infos.add(bufferInfo);
            totalBufferedPages += bufferInfo.bufferedPages();
        }

        return new OutputBufferInfo(
                "PARTITIONED",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.sum(),
                totalPagesAdded.sum(),
                Optional.of(infos.build()),
                Optional.of(new TDigestHistogram(memoryManager.getUtilizationHistogram())),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public BufferState getState()
    {
        return stateMachine.getState();
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (stateMachine.getState().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    @Override
    public ListenableFuture<Void> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    @Override
    public void enqueue(List<Slice> pages)
    {
        checkState(partitionBuffers.size() == 1, "Expected exactly one partition");
        enqueue(0, pages);
    }

    @Override
    public void enqueue(int partitionNumber, List<Slice> pages)
    {
        requireNonNull(pages, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!stateMachine.getState().canAddPages()) {
            return;
        }

        ImmutableList.Builder<SerializedPageReference> references = ImmutableList.builderWithExpectedSize(pages.size());
        long bytesAdded = 0;
        long rowCount = 0;
        for (Slice page : pages) {
            bytesAdded += page.getRetainedSize();
            int positionCount = getSerializedPagePositionCount(page);
            rowCount += positionCount;
            // create page reference counts with an initial single reference
            references.add(new SerializedPageReference(page, positionCount, 1));
        }
        List<SerializedPageReference> serializedPageReferences = references.build();

        // update stats
        totalRowsAdded.add(rowCount);
        totalPagesAdded.add(serializedPageReferences.size());

        // reserve memory
        memoryManager.updateMemoryUsage(bytesAdded);

        // add pages to the buffers (this will increase the reference count by one for each buffer)
        for (ClientBuffer buffer : partitionBuffers.get(partitionNumber)) {
            buffer.enqueuePages(serializedPageReferences);
        }

        // drop the initial reference
        dereferencePages(serializedPageReferences, onPagesReleased);
    }

    @Override
    public ListenableFuture<BufferResult> get(OutputBufferId outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return partitions.get(outputBufferId.id()).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(OutputBufferId outputBufferId, long sequenceId)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");

        partitions.get(outputBufferId.id()).acknowledgePages(sequenceId);
    }

    @Override
    public void destroy(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        partitions.get(bufferId.id()).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        stateMachine.noMorePages();
        memoryManager.setNoBlockOnFull();

        partitions.forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (stateMachine.finish()) {
            partitions.forEach(ClientBuffer::destroy);
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void abort()
    {
        // ignore abort if the buffer already in a terminal state.
        if (stateMachine.abort()) {
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
            // DO NOT destroy buffers or set no more pages.  The coordinator manages the teardown of failed queries.
        }
    }

    @Override
    public long getPeakMemoryUsage()
    {
        return memoryManager.getPeakMemoryUsage();
    }

    @Override
    public Optional<Throwable> getFailureCause()
    {
        return stateMachine.getFailureCause();
    }

    @VisibleForTesting
    void forceFreeMemory()
    {
        memoryManager.close();
    }

    private void checkFlushComplete()
    {
        BufferState state = stateMachine.getState();
        if (state != FLUSHING && state != NO_MORE_BUFFERS) {
            return;
        }

        if (partitions.stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }
}
