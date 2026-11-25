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
package io.trino.operator.scalar;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

final class TestVectorizedArrayVectorFunctions
{
    private static final int[] LENGTHS = {0, 1, 3, 4, 15, 16, 17, 31, 32, 33, 100, 768, 1536, 1537};

    @Test
    void testMatchesScalarImplementation()
    {
        Random random = new Random(42);
        for (int length : LENGTHS) {
            // arrays are regions of a larger elements block, so the raw values start at an offset
            Block first = randomDoubles(random, length + 5).getRegion(3, length);
            Block second = randomDoubles(random, length + 7).getRegion(5, length);
            assertMatches(first, second);
        }
    }

    @Test
    void testNulls()
    {
        Random random = new Random(42);
        Block values = randomDoubles(random, 100);

        BlockBuilder withNulls = DOUBLE.createFixedSizeBlockBuilder(101);
        withNulls.appendNull();
        for (int i = 1; i < 101; i++) {
            if (i % 7 == 0) {
                withNulls.appendNull();
            }
            else {
                DOUBLE.writeDouble(withNulls, random.nextDouble());
            }
        }
        Block block = withNulls.build();
        assertMatches(values, block.getRegion(1, 100));

        // block readers create blocks that may have nulls even if they do not
        Block withoutNulls = randomDoubles(random, 1, 70).getRegion(1, 70);
        assertThat(withoutNulls.mayHaveNull()).isTrue();
        assertMatches(randomDoubles(random, 70), withoutNulls);
    }

    @Test
    void testFallbackToScalarImplementation()
    {
        Random random = new Random(42);
        Block values = randomDoubles(random, 100);
        int[] ids = new int[100];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = random.nextInt(10);
        }
        assertMatches(values, DictionaryBlock.create(ids.length, randomDoubles(random, 10), ids));
    }

    @Test
    void testSpecialValues()
    {
        Block values = randomDoubles(new Random(42), 64);
        for (double specialValue : new double[] {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(64);
            for (int i = 0; i < 64; i++) {
                DOUBLE.writeDouble(builder, i == 37 ? specialValue : 0.5);
            }
            assertMatches(values, builder.build());
        }
    }

    @Test
    void testFailures()
    {
        Block first = randomDoubles(new Random(42), 64);
        Block second = randomDoubles(new Random(43), 65);
        assertThatThrownBy(() -> VectorizedArrayVectorFunctions.dotProduct(first, second))
                .isInstanceOf(TrinoException.class)
                .hasMessage("The arguments must have the same length");

        BlockBuilder zeros = DOUBLE.createFixedSizeBlockBuilder(64);
        for (int i = 0; i < 64; i++) {
            DOUBLE.writeDouble(zeros, 0);
        }
        assertThatThrownBy(() -> VectorizedArrayVectorFunctions.cosineSimilarity(first, zeros.build()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Vector magnitude cannot be zero");
    }

    private static void assertMatches(Block first, Block second)
    {
        assertClose(VectorizedArrayVectorFunctions.dotProduct(first, second), ArrayVectorFunctions.dotProduct(first, second));
        assertClose(VectorizedArrayVectorFunctions.euclideanDistance(first, second), ArrayVectorFunctions.euclideanDistance(first, second));
        if (first.getPositionCount() > 0) {
            assertClose(VectorizedArrayVectorFunctions.cosineSimilarity(first, second), ArrayVectorFunctions.cosineSimilarity(first, second));
            assertClose(VectorizedArrayVectorFunctions.cosineDistance(first, second), ArrayVectorFunctions.cosineDistance(first, second));
        }
    }

    private static void assertClose(Double actual, Double expected)
    {
        if (expected == null) {
            assertThat(actual).isNull();
            return;
        }
        if (expected.isNaN() || expected.isInfinite()) {
            // compared as objects, so that NaN is equal to NaN
            assertThat(actual).isEqualTo((Object) expected);
            return;
        }
        // the vectorized implementation sums in a different order and uses fused multiply-add
        assertThat(actual).isCloseTo(expected, within(1e-12 * Math.max(1, Math.abs(expected))));
    }

    private static Block randomDoubles(Random random, int count)
    {
        return randomDoubles(random, 0, count);
    }

    private static Block randomDoubles(Random random, int leadingNulls, int count)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(leadingNulls + count);
        for (int i = 0; i < leadingNulls; i++) {
            builder.appendNull();
        }
        for (int i = 0; i < count; i++) {
            DOUBLE.writeDouble(builder, random.nextDouble() * 2 - 1);
        }
        return builder.build();
    }
}
