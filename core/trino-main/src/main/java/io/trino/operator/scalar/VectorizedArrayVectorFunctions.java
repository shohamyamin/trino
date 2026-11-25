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

import com.google.common.base.StandardSystemProperty;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.MachineInfo.readCpuFlags;
import static java.lang.Double.longBitsToDouble;
import static java.util.Locale.ENGLISH;
import static jdk.incubator.vector.VectorOperators.ADD;

/**
 * Implementation of the functions in {@link ArrayVectorFunctions} using the Vector API. The values of a DOUBLE block
 * are the bits of the doubles in a long array, which are read into vector registers and reinterpreted as doubles, and
 * the sums are kept in independent vector accumulators. Like the scalar implementation, the values at null positions
 * are read as they are stored. Arrays that are short or are not backed by a long array use the scalar implementation.
 */
public final class VectorizedArrayVectorFunctions
{
    private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = VectorSpecies.of(double.class, LONG_SPECIES.vectorShape());
    private static final int LANES = LONG_SPECIES.length();
    // shorter arrays are faster with the scalar implementation
    private static final int MIN_VECTORIZED_LENGTH = 4 * LANES;

    private VectorizedArrayVectorFunctions() {}

    /**
     * The vectorized implementation is validated to be faster than the scalar one on x86 CPUs with vector registers
     * of at least 256 bits (AVX2 and AVX-512). It requires hardware fused multiply-add, because the Vector API emulates
     * the operation in software otherwise, which is much slower than the scalar implementation.
     */
    public static boolean isSupported()
    {
        String architecture = StandardSystemProperty.OS_ARCH.value();
        if (architecture == null) {
            return false;
        }
        architecture = architecture.toLowerCase(ENGLISH);
        boolean x86 = architecture.contains("amd64") || architecture.contains("x86");
        return x86 && LONG_SPECIES.vectorBitSize() >= 256 && readCpuFlags().contains("fma");
    }

    @Description("Calculates the euclidean distance between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double euclideanDistance(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        checkCondition(first.getPositionCount() == second.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "The arguments must have the same length");
        if (!isVectorizable(first, second)) {
            return ArrayVectorFunctions.euclideanDistance(first, second);
        }

        LongArrayBlock left = (LongArrayBlock) first;
        LongArrayBlock right = (LongArrayBlock) second;
        long[] leftValues = left.getRawValues();
        long[] rightValues = right.getRawValues();
        int leftOffset = left.getRawValuesOffset();
        int rightOffset = right.getRawValuesOffset();
        int length = left.getPositionCount();

        DoubleVector sum0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sum1 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sum2 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sum3 = DoubleVector.zero(DOUBLE_SPECIES);
        int index = 0;
        for (; index + 4 * LANES <= length; index += 4 * LANES) {
            DoubleVector difference0 = load(leftValues, leftOffset + index).sub(load(rightValues, rightOffset + index));
            DoubleVector difference1 = load(leftValues, leftOffset + index + LANES).sub(load(rightValues, rightOffset + index + LANES));
            DoubleVector difference2 = load(leftValues, leftOffset + index + 2 * LANES).sub(load(rightValues, rightOffset + index + 2 * LANES));
            DoubleVector difference3 = load(leftValues, leftOffset + index + 3 * LANES).sub(load(rightValues, rightOffset + index + 3 * LANES));
            sum0 = difference0.fma(difference0, sum0);
            sum1 = difference1.fma(difference1, sum1);
            sum2 = difference2.fma(difference2, sum2);
            sum3 = difference3.fma(difference3, sum3);
        }
        for (; index + LANES <= length; index += LANES) {
            DoubleVector difference = load(leftValues, leftOffset + index).sub(load(rightValues, rightOffset + index));
            sum0 = difference.fma(difference, sum0);
        }
        double sum = sum0.add(sum1).add(sum2.add(sum3)).reduceLanes(ADD);
        for (; index < length; index++) {
            double difference = longBitsToDouble(leftValues[leftOffset + index]) - longBitsToDouble(rightValues[rightOffset + index]);
            sum += difference * difference;
        }
        return Math.sqrt(sum);
    }

    @Description("Calculates the dot product between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double dotProduct(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        checkCondition(first.getPositionCount() == second.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "The arguments must have the same length");
        if (!isVectorizable(first, second)) {
            return ArrayVectorFunctions.dotProduct(first, second);
        }

        LongArrayBlock left = (LongArrayBlock) first;
        LongArrayBlock right = (LongArrayBlock) second;
        return dotProduct(left.getRawValues(), left.getRawValuesOffset(), right.getRawValues(), right.getRawValuesOffset(), left.getPositionCount());
    }

    @Description("Calculates the cosine similarity between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double cosineSimilarity(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        checkCondition(first.getPositionCount() == second.getPositionCount(), INVALID_FUNCTION_ARGUMENT, "The arguments must have the same length");
        if (!isVectorizable(first, second)) {
            return ArrayVectorFunctions.cosineSimilarity(first, second);
        }
        if (first.hasNull() || second.hasNull()) {
            return null;
        }

        LongArrayBlock left = (LongArrayBlock) first;
        LongArrayBlock right = (LongArrayBlock) second;
        long[] leftValues = left.getRawValues();
        long[] rightValues = right.getRawValues();
        int leftOffset = left.getRawValuesOffset();
        int rightOffset = right.getRawValuesOffset();
        int length = left.getPositionCount();

        DoubleVector firstMagnitude0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector firstMagnitude1 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector secondMagnitude0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector secondMagnitude1 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector dotProduct0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector dotProduct1 = DoubleVector.zero(DOUBLE_SPECIES);
        int index = 0;
        for (; index + 2 * LANES <= length; index += 2 * LANES) {
            DoubleVector firstValues0 = load(leftValues, leftOffset + index);
            DoubleVector firstValues1 = load(leftValues, leftOffset + index + LANES);
            DoubleVector secondValues0 = load(rightValues, rightOffset + index);
            DoubleVector secondValues1 = load(rightValues, rightOffset + index + LANES);
            firstMagnitude0 = firstValues0.fma(firstValues0, firstMagnitude0);
            firstMagnitude1 = firstValues1.fma(firstValues1, firstMagnitude1);
            secondMagnitude0 = secondValues0.fma(secondValues0, secondMagnitude0);
            secondMagnitude1 = secondValues1.fma(secondValues1, secondMagnitude1);
            dotProduct0 = firstValues0.fma(secondValues0, dotProduct0);
            dotProduct1 = firstValues1.fma(secondValues1, dotProduct1);
        }
        double firstMagnitude = firstMagnitude0.add(firstMagnitude1).reduceLanes(ADD);
        double secondMagnitude = secondMagnitude0.add(secondMagnitude1).reduceLanes(ADD);
        double dotProduct = dotProduct0.add(dotProduct1).reduceLanes(ADD);
        for (; index < length; index++) {
            double firstValue = longBitsToDouble(leftValues[leftOffset + index]);
            double secondValue = longBitsToDouble(rightValues[rightOffset + index]);
            firstMagnitude += firstValue * firstValue;
            secondMagnitude += secondValue * secondValue;
            dotProduct += firstValue * secondValue;
        }

        checkCondition(firstMagnitude != 0 && secondMagnitude != 0, INVALID_FUNCTION_ARGUMENT, "Vector magnitude cannot be zero");
        return dotProduct / Math.sqrt(firstMagnitude * secondMagnitude);
    }

    @Description("Calculates the cosine distance between two vectors")
    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    @SqlNullable
    public static Double cosineDistance(@SqlType("array(double)") Block first, @SqlType("array(double)") Block second)
    {
        Double cosineSimilarity = cosineSimilarity(first, second);
        if (cosineSimilarity == null) {
            return null;
        }
        return 1.0 - cosineSimilarity;
    }

    private static boolean isVectorizable(Block first, Block second)
    {
        return first.getPositionCount() >= MIN_VECTORIZED_LENGTH &&
                first instanceof LongArrayBlock &&
                second instanceof LongArrayBlock;
    }

    private static double dotProduct(long[] leftValues, int leftOffset, long[] rightValues, int rightOffset, int length)
    {
        DoubleVector sum0 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sum1 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sum2 = DoubleVector.zero(DOUBLE_SPECIES);
        DoubleVector sum3 = DoubleVector.zero(DOUBLE_SPECIES);
        int index = 0;
        for (; index + 4 * LANES <= length; index += 4 * LANES) {
            sum0 = load(leftValues, leftOffset + index).fma(load(rightValues, rightOffset + index), sum0);
            sum1 = load(leftValues, leftOffset + index + LANES).fma(load(rightValues, rightOffset + index + LANES), sum1);
            sum2 = load(leftValues, leftOffset + index + 2 * LANES).fma(load(rightValues, rightOffset + index + 2 * LANES), sum2);
            sum3 = load(leftValues, leftOffset + index + 3 * LANES).fma(load(rightValues, rightOffset + index + 3 * LANES), sum3);
        }
        for (; index + LANES <= length; index += LANES) {
            sum0 = load(leftValues, leftOffset + index).fma(load(rightValues, rightOffset + index), sum0);
        }
        double sum = sum0.add(sum1).add(sum2.add(sum3)).reduceLanes(ADD);
        for (; index < length; index++) {
            sum += longBitsToDouble(leftValues[leftOffset + index]) * longBitsToDouble(rightValues[rightOffset + index]);
        }
        return sum;
    }

    private static DoubleVector load(long[] values, int offset)
    {
        return LongVector.fromArray(LONG_SPECIES, values, offset).reinterpretAsDoubles();
    }
}
