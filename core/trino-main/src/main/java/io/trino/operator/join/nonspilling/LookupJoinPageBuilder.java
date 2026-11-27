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
package io.trino.operator.join.nonspilling;

import io.trino.operator.join.LookupSource;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * This page builder creates pages with dictionary blocks:
 * normal dictionary blocks for the probe side and the original blocks for the build side.
 * <p>
 * Build rows are buffered and copied one column at a time when the page is built, so values
 * can be copied in bulk for each build block instead of row by row.
 * <p>
 * TODO use dictionary blocks (probably extended kind) to avoid data copying for build side
 */
public class LookupJoinPageBuilder
{
    // number of build rows copied before the build row size is known, to estimate the output page size
    private static final int INITIAL_BUILD_BATCH_SIZE = 64;

    private final IntArrayList probeIndexBuilder = new IntArrayList();
    private final PageBuilder buildPageBuilder;
    private final int buildOutputChannelCount;
    // join positions of the build rows that are not copied yet, a negative value is a null build row
    private final LongArrayList pendingBuildPositions = new LongArrayList();
    // position in buildPageBuilder of the build values of each copied output row
    private final IntArrayList buildRowIndexes = new IntArrayList();
    private boolean buildRowsReordered;
    private LookupSource lookupSource;
    private long estimatedBuildRowSize = -1;
    private long estimatedProbeBlockBytes;
    private long estimatedProbeRowSize = -1;
    private int previousPosition = -1;
    private boolean isSequentialProbeIndices = true;
    private boolean repeatBuildRow;

    public LookupJoinPageBuilder(List<Type> buildTypes)
    {
        this.buildPageBuilder = new PageBuilder(requireNonNull(buildTypes, "buildTypes is null"));
        this.buildOutputChannelCount = buildTypes.size();
    }

    public boolean isFull()
    {
        long estimatedBuildBytes = buildPageBuilder.getSizeInBytes() + pendingBuildPositions.size() * max(estimatedBuildRowSize, 0);
        return estimatedProbeBlockBytes + estimatedBuildBytes >= DEFAULT_MAX_PAGE_SIZE_IN_BYTES ||
                probeIndexBuilder.size() >= MAX_BATCH_SIZE ||
                buildPageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return probeIndexBuilder.isEmpty() && buildPageBuilder.isEmpty();
    }

    public int getPositionCount()
    {
        // when build rows are repeated then position count is equal to probe position count
        verify(!repeatBuildRow);
        return probeIndexBuilder.size();
    }

    public void reset()
    {
        // be aware that probeIndexBuilder will not clear its capacity
        probeIndexBuilder.clear();
        buildPageBuilder.reset();
        pendingBuildPositions.clear();
        buildRowIndexes.clear();
        buildRowsReordered = false;
        estimatedProbeBlockBytes = 0;
        estimatedProbeRowSize = -1;
        previousPosition = -1;
        isSequentialProbeIndices = true;
        repeatBuildRow = false;
    }

    /**
     * append the index for the probe and the join position of the row for the build
     */
    public void appendRow(JoinProbe probe, LookupSource lookupSource, long joinPosition)
    {
        verify(joinPosition >= 0);
        // probe side
        appendProbeIndex(probe);

        // build side
        this.lookupSource = lookupSource;
        appendBuildPosition(joinPosition);
    }

    /**
     * append the index for the probe and append nulls for the build
     */
    public void appendNullForBuild(JoinProbe probe)
    {
        // probe side
        appendProbeIndex(probe);

        // build side
        appendBuildPosition(-1);
    }

    public void repeatBuildRow()
    {
        repeatBuildRow = true;
    }

    public Page build(JoinProbe probe)
    {
        copyBuildRows();
        if (repeatBuildRow) {
            return buildRepeatedPage(probe);
        }

        int outputPositions = probeIndexBuilder.size();
        verify(buildPageBuilder.getPositionCount() == outputPositions);

        int[] probeOutputChannels = probe.getOutputChannels();
        Block[] blocks = new Block[probeOutputChannels.length + buildOutputChannelCount];
        Page probePage = probe.getPage();
        if (!isSequentialProbeIndices || outputPositions == 0) {
            int[] probeIndices = probeIndexBuilder.toIntArray();
            for (int i = 0; i < probeOutputChannels.length; i++) {
                blocks[i] = probePage.getBlock(probeOutputChannels[i]).getPositions(probeIndices, 0, outputPositions);
            }
        }
        else {
            // probeIndices are sequential without holes
            int startRegion = probeIndexBuilder.getInt(0);
            verify(previousPosition - startRegion == outputPositions - 1);
            // probeIndices are a simple covering of the block, output the probe block directly
            boolean outputProbeBlocksDirectly = startRegion == 0 && outputPositions == probePage.getPositionCount();

            for (int i = 0; i < probeOutputChannels.length; i++) {
                Block block = probePage.getBlock(probeOutputChannels[i]);
                if (!outputProbeBlocksDirectly) {
                    // only a subregion of the block should be output
                    block = block.getRegion(startRegion, outputPositions);
                }
                blocks[i] = block;
            }
        }

        int offset = probeOutputChannels.length;
        for (int i = 0; i < buildOutputChannelCount; i++) {
            Block block = buildPageBuilder.getBlockBuilder(i).build();
            if (buildRowsReordered) {
                // build rows were copied grouped by build block, restore the output row order
                block = block.copyPositions(buildRowIndexes.elements(), 0, outputPositions);
            }
            blocks[offset + i] = block;
            verify(blocks[offset + i].getPositionCount() == outputPositions);
        }
        return new Page(outputPositions, blocks);
    }

    private Page buildRepeatedPage(JoinProbe probe)
    {
        // Build match can be repeated only if there is a single build row match
        // and probe join channels are run length encoded.
        verify(probe.areProbeJoinChannelsRunLengthEncoded());
        verify(buildPageBuilder.getPositionCount() == 1);
        verify(probeIndexBuilder.size() == 1);
        verify(probeIndexBuilder.getInt(0) == 0);

        int positionCount = probe.getPage().getPositionCount();
        int[] probeOutputChannels = probe.getOutputChannels();
        Block[] blocks = new Block[probeOutputChannels.length + buildOutputChannelCount];

        for (int i = 0; i < probeOutputChannels.length; i++) {
            blocks[i] = probe.getPage().getBlock(probeOutputChannels[i]);
        }

        int offset = probeOutputChannels.length;
        for (int i = 0; i < buildOutputChannelCount; i++) {
            Block buildBlock = buildPageBuilder.getBlockBuilder(i).build();
            blocks[offset + i] = RunLengthEncodedBlock.create(buildBlock, positionCount);
        }

        return new Page(positionCount, blocks);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("estimatedSize", estimatedProbeBlockBytes + buildPageBuilder.getSizeInBytes())
                .add("positionCount", probeIndexBuilder.size())
                .toString();
    }

    private void appendBuildPosition(long joinPosition)
    {
        pendingBuildPositions.add(joinPosition);
        if (estimatedBuildRowSize < 0 && pendingBuildPositions.size() >= INITIAL_BUILD_BATCH_SIZE) {
            copyBuildRows();
        }
    }

    private void copyBuildRows()
    {
        int rowCount = pendingBuildPositions.size();
        if (rowCount == 0) {
            return;
        }
        long[] joinPositions = pendingBuildPositions.elements();
        int firstRowIndex = buildPageBuilder.getPositionCount();

        int nullCount = 0;
        for (int i = 0; i < rowCount; i++) {
            if (joinPositions[i] < 0) {
                nullCount++;
            }
        }
        int matchCount = rowCount - nullCount;

        // null rows are appended first, followed by the matched rows in the order chosen by the lookup source
        for (int channel = 0; channel < buildOutputChannelCount; channel++) {
            BlockBuilder blockBuilder = buildPageBuilder.getBlockBuilder(channel);
            for (int i = 0; i < nullCount; i++) {
                blockBuilder.appendNull();
            }
        }

        buildRowIndexes.size(firstRowIndex + rowCount);
        int[] rowIndexes = buildRowIndexes.elements();
        long[] matchPositions = joinPositions;
        int[] matchRows = null;
        if (nullCount > 0) {
            matchPositions = new long[matchCount];
            matchRows = new int[matchCount];
            int nullIndex = 0;
            int matchIndex = 0;
            for (int i = 0; i < rowCount; i++) {
                if (joinPositions[i] < 0) {
                    rowIndexes[firstRowIndex + i] = firstRowIndex + nullIndex;
                    nullIndex++;
                }
                else {
                    matchPositions[matchIndex] = joinPositions[i];
                    matchRows[matchIndex] = i;
                    matchIndex++;
                }
            }
        }
        if (matchCount > 0) {
            int[] appendedRows = new int[matchCount];
            lookupSource.appendTo(matchPositions, 0, matchCount, buildPageBuilder, 0, appendedRows);
            for (int i = 0; i < matchCount; i++) {
                int row = matchRows == null ? appendedRows[i] : matchRows[appendedRows[i]];
                rowIndexes[firstRowIndex + row] = firstRowIndex + nullCount + i;
            }
        }
        buildPageBuilder.declarePositions(rowCount);

        for (int i = firstRowIndex; i < firstRowIndex + rowCount; i++) {
            buildRowsReordered |= rowIndexes[i] != i;
        }
        pendingBuildPositions.clear();
        estimatedBuildRowSize = buildPageBuilder.getSizeInBytes() / buildPageBuilder.getPositionCount();
    }

    private void appendProbeIndex(JoinProbe probe)
    {
        int position = probe.getPosition();
        // positions to be appended should be in ascending order
        verify(position >= 0 && previousPosition <= position);
        isSequentialProbeIndices &= position == previousPosition + 1 || previousPosition == -1;

        // Update probe indices and size
        probeIndexBuilder.add(position);
        estimatedProbeBlockBytes += Integer.BYTES;

        // Update memory usage for probe side.
        //
        // The size of the probe cannot be easily calculated given
        // (1) the structure of Block is recursive,
        // (2) an inner block can serve as multiple views (e.g., in a dictionary block).
        //     Without a dedup at the granularity of rows, we cannot tell if we are overcounting, and
        // (3) even we are able to dedup magically, calling getRegionSizeInBytes can be expensive.
        //     For example, consider a dictionary block inside an array block;
        //     calling getRegionSizeInBytes(p, 1) of the array block can lead to calling getRegionSizeInBytes with an arbitrary length for the dictionary block,
        //     which is very expensive.
        //
        // To workaround the memory accounting complexity yet having a relatively reasonable estimation, we use sizeInBytes / positionCount as the size for each row.
        // It can be shown that the output page is bounded within range [buildPageBuilder.getSizeInBytes(), buildPageBuilder.getSizeInBytes + probe.getPage().getSizeInBytes()].
        //
        // This is under the assumption that the position of a probe is non-decreasing.
        // if position > previousPosition, we know it is a new row to append and we accumulate the estimated row size (sizeInBytes / positionCount);
        // otherwise we do not count because we know it is duplicated with the previous appended row.
        // So in the worst case, we can only accumulate up to the sizeInBytes of the probe page.
        //
        // On the other hand, we do not want to produce a page that is too small if the build size is too small (e.g., the build side is with all nulls).
        // That means we only appended a few small rows in the probe and reached the probe end.
        // But that is going to happen anyway because we have to flush the page whenever we reach the probe end.
        // So with or without precise memory accounting, the output page is small anyway.

        if (previousPosition != position) {
            previousPosition = position;
            estimatedProbeBlockBytes += getEstimatedProbeRowSize(probe);
        }
    }

    private long getEstimatedProbeRowSize(JoinProbe probe)
    {
        if (estimatedProbeRowSize != -1) {
            return estimatedProbeRowSize;
        }

        long estimatedProbeRowSize = 0;
        for (int index : probe.getOutputChannels()) {
            Block block = probe.getPage().getBlock(index);
            // Estimate the size of the probe row
            estimatedProbeRowSize += block.getSizeInBytes() / block.getPositionCount();
        }

        this.estimatedProbeRowSize = estimatedProbeRowSize;
        return estimatedProbeRowSize;
    }
}
