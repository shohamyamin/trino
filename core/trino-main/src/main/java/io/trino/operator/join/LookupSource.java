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
package io.trino.operator.join;

import io.trino.annotation.NotThreadSafe;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;

import java.io.Closeable;

@NotThreadSafe
public interface LookupSource
        extends Closeable
{
    long getInMemorySizeInBytes();

    long getJoinPositionCount();

    long joinPositionWithinPartition(long joinPosition);

    /**
     * The `rawHashes` and `result` arrays are global to the entire processed page (thus, the same size),
     * while the `positions` array may hold any number of selected positions from this page
     */
    default void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] rawHashes, long[] result)
    {
        for (int i = 0; i < positions.length; i++) {
            result[positions[i]] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage, rawHashes[positions[i]]);
        }
    }

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash);

    /**
     * The `result` array is global to the entire processed page, while the `positions` array may hold
     * any number of selected positions from this page
     */
    default void getJoinPosition(int[] positions, Page hashChannelsPage, Page allChannelsPage, long[] result)
    {
        for (int i = 0; i < positions.length; i++) {
            result[positions[i]] = getJoinPosition(positions[i], hashChannelsPage, allChannelsPage);
        }
    }

    long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage);

    long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset);

    /**
     * Appends the rows at the join positions {@code positions[offset]} to {@code positions[offset + length - 1]}
     * to the page builder starting at {@code outputChannelOffset}, without declaring positions. The rows can be
     * appended in any order, so implementations can copy values in bulk: on return, {@code appendedRows[offset + k]}
     * is the index in {@code positions} of the k-th appended row.
     */
    default void appendTo(long[] positions, int offset, int length, PageBuilder pageBuilder, int outputChannelOffset, int[] appendedRows)
    {
        for (int i = offset; i < offset + length; i++) {
            appendTo(positions[i], pageBuilder, outputChannelOffset);
            appendedRows[i] = i;
        }
    }

    boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage);

    boolean isEmpty();

    @Override
    void close();
}
