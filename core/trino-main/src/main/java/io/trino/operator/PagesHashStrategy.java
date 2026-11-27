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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import java.util.Arrays;

import static java.lang.Math.max;

public interface PagesHashStrategy
{
    /**
     * Gets the number of columns appended by this PagesHashStrategy.
     */
    int getChannelCount();

    /**
     * Get the total of allocated size
     */
    long getSizeInBytes();

    /**
     * Appends all values at the specified position to the page builder starting at {@code outputChannelOffset}.
     */
    void appendTo(int blockIndex, int position, PageBuilder pageBuilder, int outputChannelOffset);

    /**
     * Appends all values of rows grouped by block to the page builder starting at {@code outputChannelOffset},
     * without declaring positions. The rows of block {@code b} are at {@code positions[blockOffsets[b]]}
     * to {@code positions[blockOffsets[b + 1] - 1]}.
     */
    void appendBlockGroupsTo(int[] blockOffsets, int[] positions, PageBuilder pageBuilder, int outputChannelOffset);

    /**
     * Appends all values of the {@code length} rows at {@code (blockIndexes[i], positions[i])} to the page builder
     * starting at {@code outputChannelOffset}, without declaring positions. The rows can be appended in any order:
     * on return, {@code appendedRows[k]} is the index {@code i} of the k-th appended row.
     */
    default void appendTo(int[] blockIndexes, int[] positions, int length, PageBuilder pageBuilder, int outputChannelOffset, int[] appendedRows)
    {
        int blockCount = 0;
        for (int i = 0; i < length; i++) {
            blockCount = max(blockCount, blockIndexes[i] + 1);
        }
        if (getChannelCount() == 0 || length < 2 * blockCount) {
            // too few rows per block for bulk copies to pay off, so copy row by row in the given order
            for (int i = 0; i < length; i++) {
                appendTo(blockIndexes[i], positions[i], pageBuilder, outputChannelOffset);
                appendedRows[i] = i;
            }
            return;
        }

        // group the rows by block with a stable counting sort, so each column is copied with one bulk call per block
        int[] blockOffsets = new int[blockCount + 1];
        for (int i = 0; i < length; i++) {
            blockOffsets[blockIndexes[i] + 1]++;
        }
        for (int block = 0; block < blockCount; block++) {
            blockOffsets[block + 1] += blockOffsets[block];
        }
        int[] nextOffsets = Arrays.copyOf(blockOffsets, blockCount);
        int[] groupedPositions = new int[length];
        for (int i = 0; i < length; i++) {
            int index = nextOffsets[blockIndexes[i]]++;
            appendedRows[index] = i;
            groupedPositions[index] = positions[i];
        }
        appendBlockGroupsTo(blockOffsets, groupedPositions, pageBuilder, outputChannelOffset);
    }

    /**
     * Calculates the hash code the hashed columns in this PagesHashStrategy at the specified position.
     */
    long hashPosition(int blockIndex, int position);

    /**
     * Calculates the hash code at {@code position} in {@code page}. Page must have the same number of
     * Blocks as the hashed columns and each entry is expected to be the same type.
     */
    long hashRow(int position, Page page);

    /**
     * Compares the values in the specified pages. The values are compared positionally, so {@code leftPage}
     * and {@code rightPage} must have the same number of entries as the hashed columns and each entry
     * is expected to be the same type. The values are compared under "not distinct from" semantics.
     */
    boolean rowIdenticalToRow(int leftPosition, Page leftPage, int rightPosition, Page rightPage);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the values in the specified page. The
     * values are compared positionally, so {@code rightPage} must have the same number of entries as
     * the hashed columns and each entry is expected to be the same type.
     * {@code rightPage} is used if join uses filter function and must contain all columns from probe side of join.
     * The values are compared under "not distinct from" semantics.
     */
    boolean positionIdenticalToRow(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the values in the specified page. The
     * values are compared positionally, so {@code rightPage} must have the same number of entries as
     * the hashed columns and each entry is expected to be the same type.
     * {@code rightPage} is used if join uses filter function and must contain all columns from probe side of join.
     * <p>
     * This method does not perform any null checks.
     */
    boolean positionEqualsRowIgnoreNulls(int leftBlockIndex, int leftPosition, int rightPosition, Page rightPage);

    /**
     * Compares the hashed columns in this PagesHashStrategy to the hashed columns in the Page.
     * The values are compared positionally under "not distinct from" semantics.
     * {@code rightChannels} must have the same number of entries as the hashed columns
     * and each entry is expected to be the same type.
     */
    boolean positionIdenticalToRow(int leftBlockIndex, int leftPosition, int rightPosition, Page page, int[] rightChannels);

    /**
     * Compares the hashed columns in this PagesHashStrategy at the specified positions.
     * The values are compared under "not distinct from" semantics.
     */
    boolean positionIdenticalToPosition(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition);

    /**
     * Compares the hashed columns in this PagesHashStrategy at the specified positions.
     * <p>
     * This method does not perform any null checks.
     */
    boolean positionEqualsPositionIgnoreNulls(int leftBlockIndex, int leftPosition, int rightBlockIndex, int rightPosition);

    /**
     * Checks if any of the hashed columns is null
     */
    boolean isPositionNull(int blockIndex, int blockPosition);

    /**
     * Compares sort channel (if applicable) values at the specified positions.
     */
    int compareSortChannelPositions(int leftBlockIndex, int leftBlockPosition, int rightBlockIndex, int rightBlockPosition);

    /**
     * Checks if sort channel is null at the specified position
     */
    boolean isSortChannelPositionNull(int blockIndex, int blockPosition);
}
