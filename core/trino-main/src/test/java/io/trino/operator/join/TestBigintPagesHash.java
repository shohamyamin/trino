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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.operator.SimplePagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigintPagesHash
{
    private static final int POSITION_COUNT = 1000;
    private static final int DISTINCT_VALUES = 700;

    @Test
    public void testLookup()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(POSITION_COUNT);
        for (int position = 0; position < POSITION_COUNT; position++) {
            if (position % 50 == 0) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, position % DISTINCT_VALUES);
            }
        }
        Block block = blockBuilder.build();

        LongArrayList addresses = new LongArrayList();
        for (int position = 0; position < POSITION_COUNT; position++) {
            addresses.add(encodeSyntheticAddress(0, position));
        }
        ObjectArrayList<Block> channel = new ObjectArrayList<>();
        channel.add(block);
        SimplePagesHashStrategy pagesHashStrategy = new SimplePagesHashStrategy(
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(channel),
                ImmutableList.of(0),
                Optional.empty(),
                new BlockTypeOperators(new TypeOperators()));

        PositionLinks.FactoryBuilder positionLinksBuilder = ArrayPositionLinks.builder(POSITION_COUNT);
        // a small hash array, so that many keys collide
        BigintPagesHash pagesHash = new BigintPagesHash(addresses, pagesHashStrategy, positionLinksBuilder, _ -> 1024, ImmutableList.of(new Page(block)), 0);
        PositionLinks positionLinks = positionLinksBuilder.build().create(ImmutableList.of());

        int probeCount = DISTINCT_VALUES + 20;
        BlockBuilder probeBuilder = BIGINT.createFixedSizeBlockBuilder(probeCount);
        for (int value = -10; value < DISTINCT_VALUES + 10; value++) {
            BIGINT.writeLong(probeBuilder, value);
        }
        Page probePage = new Page(probeBuilder.build());
        int[] batchedAddressIndexes = pagesHash.getAddressIndex(IntStream.range(0, probeCount).toArray(), probePage);

        for (int probePosition = 0; probePosition < probeCount; probePosition++) {
            long value = BIGINT.getLong(probePage.getBlock(0), probePosition);
            Set<Integer> expected = IntStream.range(0, POSITION_COUNT)
                    .filter(position -> !block.isNull(position) && BIGINT.getLong(block, position) == value)
                    .boxed()
                    .collect(toImmutableSet());

            int addressIndex = pagesHash.getAddressIndex(probePosition, probePage);
            assertThat(batchedAddressIndexes[probePosition]).isEqualTo(addressIndex);

            ImmutableSet.Builder<Integer> actual = ImmutableSet.builder();
            for (int position = addressIndex == -1 ? -1 : positionLinks.start(addressIndex, probePosition, probePage);
                    position != -1;
                    position = positionLinks.next(position, probePosition, probePage)) {
                actual.add(position);
            }
            assertThat(actual.build()).isEqualTo(expected);
        }
    }
}
