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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.DoubleType.DOUBLE;

/**
 * Compares the scalar and the vectorized implementations of the array vector functions.
 */
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayVectorFunctions
{
    @Param({"16", "128", "768", "1536"})
    private int dimensions = 768;

    private Block first;
    private Block second;

    @Setup
    public void setup()
    {
        Random random = new Random(42);
        first = randomDoubles(random, dimensions);
        second = randomDoubles(random, dimensions);
    }

    @Benchmark
    public double scalarDotProduct()
    {
        return ArrayVectorFunctions.dotProduct(first, second);
    }

    @Benchmark
    public double vectorizedDotProduct()
    {
        return VectorizedArrayVectorFunctions.dotProduct(first, second);
    }

    @Benchmark
    public Double scalarCosineSimilarity()
    {
        return ArrayVectorFunctions.cosineSimilarity(first, second);
    }

    @Benchmark
    public Double vectorizedCosineSimilarity()
    {
        return VectorizedArrayVectorFunctions.cosineSimilarity(first, second);
    }

    @Benchmark
    public double scalarEuclideanDistance()
    {
        return ArrayVectorFunctions.euclideanDistance(first, second);
    }

    @Benchmark
    public double vectorizedEuclideanDistance()
    {
        return VectorizedArrayVectorFunctions.euclideanDistance(first, second);
    }

    private static Block randomDoubles(Random random, int count)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            DOUBLE.writeDouble(builder, random.nextDouble());
        }
        return builder.build();
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkArrayVectorFunctions.class).run();
    }
}
