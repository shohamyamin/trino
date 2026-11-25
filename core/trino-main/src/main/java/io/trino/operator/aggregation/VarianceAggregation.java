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
package io.trino.operator.aggregation;

import io.trino.annotation.UsedByGeneratedCode;
import io.trino.operator.aggregation.state.VarianceState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.WindowAccumulator;
import io.trino.spi.function.WindowIndex;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;

@AggregationFunction
public final class VarianceAggregation
{
    private VarianceAggregation() {}

    @InputFunction
    public static void doubleInput(@AggregationState VarianceState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        state.update(value);
    }

    @InputFunction
    public static void bigintInput(@AggregationState VarianceState state, @SqlType(StandardTypes.BIGINT) long value)
    {
        state.update((double) value);
    }

    @CombineFunction
    public static void combine(@AggregationState VarianceState state, @AggregationState VarianceState otherState)
    {
        state.merge(otherState);
    }

    @AggregationFunction(value = "variance", alias = "var_samp", windowAccumulator = VarianceAggregation.SampleVarianceWindowAccumulator.class)
    @Description("Returns the sample variance of the argument")
    @SqlNullable
    @OutputFunction(StandardTypes.DOUBLE)
    public static void variance(@AggregationState VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count < 2) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / (count - 1);
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction(value = "var_pop", windowAccumulator = VarianceAggregation.PopulationVarianceWindowAccumulator.class)
    @Description("Returns the population variance of the argument")
    @SqlNullable
    @OutputFunction(StandardTypes.DOUBLE)
    public static void variancePop(@AggregationState VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / count;
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction(value = "stddev", alias = "stddev_samp", windowAccumulator = VarianceAggregation.SampleStandardDeviationWindowAccumulator.class)
    @Description("Returns the sample standard deviation of the argument")
    @SqlNullable
    @OutputFunction(StandardTypes.DOUBLE)
    public static void stddev(@AggregationState VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count < 2) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / (count - 1);
            result = Math.sqrt(result);
            DOUBLE.writeDouble(out, result);
        }
    }

    @AggregationFunction(value = "stddev_pop", windowAccumulator = VarianceAggregation.PopulationStandardDeviationWindowAccumulator.class)
    @Description("Returns the population standard deviation of the argument")
    @SqlNullable
    @OutputFunction(StandardTypes.DOUBLE)
    public static void stddevPop(@AggregationState VarianceState state, BlockBuilder out)
    {
        long count = state.getCount();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double m2 = state.getM2();
            double result = m2 / count;
            result = Math.sqrt(result);
            DOUBLE.writeDouble(out, result);
        }
    }

    /**
     * Keeps the count, mean and sum of squared differences from the mean of a window frame, and updates them when
     * rows enter or leave the frame, so that a sliding frame costs O(1) per row instead of re-aggregating the frame.
     */
    private abstract static class VarianceWindowAccumulator
            implements WindowAccumulator
    {
        private final boolean bigintInput;
        private long count;
        private double mean;
        private double m2;

        protected VarianceWindowAccumulator(BoundSignature signature)
        {
            this.bigintInput = signature.getArgumentType(0).equals(BIGINT);
        }

        protected VarianceWindowAccumulator(VarianceWindowAccumulator other)
        {
            this.bigintInput = other.bigintInput;
            this.count = other.count;
            this.mean = other.mean;
            this.m2 = other.m2;
        }

        @Override
        public long getEstimatedSize()
        {
            return Long.BYTES + Double.BYTES + Double.BYTES;
        }

        @Override
        public void addInput(WindowIndex index, int startPosition, int endPosition)
        {
            for (int position = startPosition; position <= endPosition; position++) {
                if (!index.isNull(0, position)) {
                    double value = readValue(index, position);
                    count++;
                    double delta = value - mean;
                    mean += delta / count;
                    m2 += delta * (value - mean);
                }
            }
        }

        @Override
        public boolean removeInput(WindowIndex index, int startPosition, int endPosition)
        {
            // a non-finite input makes the state non-finite, and it cannot be recovered by removing that input
            if (!Double.isFinite(mean) || !Double.isFinite(m2)) {
                return false;
            }

            for (int position = startPosition; position <= endPosition; position++) {
                if (!index.isNull(0, position)) {
                    double value = readValue(index, position);
                    count--;
                    if (count == 0) {
                        mean = 0;
                        m2 = 0;
                    }
                    else {
                        // inverse of the update in addInput
                        double delta = value - mean;
                        mean -= delta / count;
                        m2 -= delta * (value - mean);
                    }
                }
            }

            if (count == 1) {
                // a single value has no deviation; avoid leaving a rounding residue
                m2 = 0;
            }
            // rounding can push the sum of squared differences below zero when the remaining values are (nearly) equal,
            // in which case the frame has to be re-aggregated
            return m2 >= 0;
        }

        private double readValue(WindowIndex index, int position)
        {
            if (bigintInput) {
                return index.getLong(0, position);
            }
            return index.getDouble(0, position);
        }

        protected long count()
        {
            return count;
        }

        protected double m2()
        {
            return m2;
        }
    }

    public static final class SampleVarianceWindowAccumulator
            extends VarianceWindowAccumulator
    {
        @UsedByGeneratedCode
        public SampleVarianceWindowAccumulator(BoundSignature signature)
        {
            super(signature);
        }

        private SampleVarianceWindowAccumulator(SampleVarianceWindowAccumulator other)
        {
            super(other);
        }

        @Override
        public WindowAccumulator copy()
        {
            return new SampleVarianceWindowAccumulator(this);
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            if (count() < 2) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, m2() / (count() - 1));
            }
        }
    }

    public static final class PopulationVarianceWindowAccumulator
            extends VarianceWindowAccumulator
    {
        @UsedByGeneratedCode
        public PopulationVarianceWindowAccumulator(BoundSignature signature)
        {
            super(signature);
        }

        private PopulationVarianceWindowAccumulator(PopulationVarianceWindowAccumulator other)
        {
            super(other);
        }

        @Override
        public WindowAccumulator copy()
        {
            return new PopulationVarianceWindowAccumulator(this);
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            if (count() == 0) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, m2() / count());
            }
        }
    }

    public static final class SampleStandardDeviationWindowAccumulator
            extends VarianceWindowAccumulator
    {
        @UsedByGeneratedCode
        public SampleStandardDeviationWindowAccumulator(BoundSignature signature)
        {
            super(signature);
        }

        private SampleStandardDeviationWindowAccumulator(SampleStandardDeviationWindowAccumulator other)
        {
            super(other);
        }

        @Override
        public WindowAccumulator copy()
        {
            return new SampleStandardDeviationWindowAccumulator(this);
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            if (count() < 2) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, Math.sqrt(m2() / (count() - 1)));
            }
        }
    }

    public static final class PopulationStandardDeviationWindowAccumulator
            extends VarianceWindowAccumulator
    {
        @UsedByGeneratedCode
        public PopulationStandardDeviationWindowAccumulator(BoundSignature signature)
        {
            super(signature);
        }

        private PopulationStandardDeviationWindowAccumulator(PopulationStandardDeviationWindowAccumulator other)
        {
            super(other);
        }

        @Override
        public WindowAccumulator copy()
        {
            return new PopulationStandardDeviationWindowAccumulator(this);
        }

        @Override
        public void output(BlockBuilder blockBuilder)
        {
            if (count() == 0) {
                blockBuilder.appendNull();
            }
            else {
                DOUBLE.writeDouble(blockBuilder, Math.sqrt(m2() / count()));
            }
        }
    }
}
