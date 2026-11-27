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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetReaderOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetDataSource
{
    @ParameterizedTest
    @MethodSource("testPlanReadOrderingProvider")
    public void testPlanReadOrdering(DataSize maxBufferSize)
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                ParquetReaderOptions.builder().withMaxBufferSize(maxBufferSize).build());

        ListMultimap<String, ChunkReader> chunkReaders = dataSource.planChunksRead(
                ImmutableListMultimap.<String, DiskRange>builder()
                        .putAll("test", new DiskRange(0, 200), new DiskRange(400, 100), new DiskRange(700, 200))
                        .build(),
                newSimpleAggregatedMemoryContext());
        assertThat(chunkReaders.get("test"))
                .map(ChunkReader::read)
                .isEqualTo(ImmutableList.of(
                        testingInput.slice(0, 200),
                        testingInput.slice(400, 100),
                        testingInput.slice(700, 200)));
    }

    public static Stream<DataSize> testPlanReadOrderingProvider()
    {
        return Stream.of(
                DataSize.ofBytes(200), // Mix of large and small ranges
                DataSize.ofBytes(100000000)); // All small ranges
    }

    @Test
    public void testMemoryAccounting()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                ParquetReaderOptions.builder().withMaxBufferSize(DataSize.ofBytes(500)).build());
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ListMultimap<String, ChunkReader> chunkReaders = dataSource.planChunksRead(ImmutableListMultimap.<String, DiskRange>builder()
                        .put("1", new DiskRange(0, 200))
                        .put("2", new DiskRange(400, 100))
                        .put("3", new DiskRange(700, 200))
                        .build(),
                memoryContext);

        ChunkReader firstReader = Iterables.getOnlyElement(chunkReaders.get("1"));
        ChunkReader secondReader = Iterables.getOnlyElement(chunkReaders.get("2"));
        ChunkReader thirdReader = Iterables.getOnlyElement(chunkReaders.get("3"));
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        firstReader.read();
        // first and second range are merged
        assertThat(memoryContext.getBytes()).isEqualTo(500);
        firstReader.free();
        // since the second reader is not freed, the memory is still retained
        assertThat(memoryContext.getBytes()).isEqualTo(500);
        thirdReader.read();
        // third reader is standalone so only retains its size
        assertThat(memoryContext.getBytes()).isEqualTo(700);
        thirdReader.free();
        // third reader is standalone, free releases the memory
        assertThat(memoryContext.getBytes()).isEqualTo(500);
        secondReader.read();
        // second reader is merged with the first, read only accesses already cached data
        assertThat(memoryContext.getBytes()).isEqualTo(500);
        secondReader.free();
        // both readers using merged reader are freed, all memory is released
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testChunkedInputStreamLazyLoading()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                ParquetReaderOptions.builder()
                        .withMaxBufferSize(DataSize.ofBytes(500))
                        .withMaxMergeDistance(DataSize.ofBytes(0))
                        .build());
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Map<String, ChunkedInputStream> inputStreams = dataSource.planRead(
                ImmutableListMultimap.<String, DiskRange>builder()
                        .put("1", new DiskRange(0, 200))
                        .put("1", new DiskRange(250, 50))
                        .put("2", new DiskRange(400, 100))
                        .put("2", new DiskRange(700, 200))
                        .build(),
                memoryContext);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        inputStreams.get("1").getSlice(200);
        assertThat(memoryContext.getBytes()).isEqualTo(200);

        inputStreams.get("2").getSlice(100);
        assertThat(memoryContext.getBytes()).isEqualTo(200 + 100);

        inputStreams.get("1").close();
        assertThat(memoryContext.getBytes()).isEqualTo(100);
    }

    @Test
    public void testMergeSmallReads()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                ParquetReaderOptions.builder()
                        .withMaxBufferSize(DataSize.ofBytes(500))
                        .withMaxMergeDistance(DataSize.ofBytes(300))
                        .build());
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Map<String, ChunkedInputStream> inputStreams = dataSource.planRead(
                ImmutableListMultimap.<String, DiskRange>builder()
                        .put("1", new DiskRange(0, 200))
                        .put("1", new DiskRange(250, 50))
                        .put("2", new DiskRange(400, 100))
                        .put("2", new DiskRange(600, 200))
                        .put("3", new DiskRange(1100, 50))
                        .put("3", new DiskRange(1500, 50))
                        .build(),
                memoryContext);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        inputStreams.get("1").getSlice(200);
        // Reads are merged only upto 500 bytes due to max-buffer-size
        assertThat(memoryContext.getBytes()).isEqualTo(500);

        inputStreams.get("2").getSlice(100);
        // no extra read needed
        assertThat(memoryContext.getBytes()).isEqualTo(500);

        inputStreams.get("1").close();
        inputStreams.get("2").close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        inputStreams.get("3").getSlice(50);
        // no merged read due to max-merge-distance
        assertThat(memoryContext.getBytes()).isEqualTo(50);
    }

    @Test
    public void testPrefetch()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                ParquetReaderOptions.builder()
                        .withMaxBufferSize(DataSize.ofBytes(500))
                        .withMaxMergeDistance(DataSize.ofBytes(300))
                        .build(),
                Optional.of(directExecutor()));
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Map<String, ChunkedInputStream> inputStreams = dataSource.planRead(
                ImmutableListMultimap.<String, DiskRange>builder()
                        .put("1", new DiskRange(0, 200))
                        .put("2", new DiskRange(400, 100))
                        .put("3", new DiskRange(1100, 50))
                        .put("4", new DiskRange(2000, 600))
                        .put("5", new DiskRange(3000, 100))
                        .build(),
                memoryContext);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        // "1" and "2" share a merged range, "4" is split into two ranges, and only its first one is loaded
        inputStreams.get("1").prefetch();
        inputStreams.get("2").prefetch();
        inputStreams.get("3").prefetch();
        inputStreams.get("4").prefetch();
        assertThat(dataSource.getConcurrentReads()).isEqualTo(3);
        assertThat(memoryContext.getBytes()).isEqualTo(500 + 50 + 500);

        assertThat(inputStreams.get("1").getSlice(200)).isEqualTo(testingInput.slice(0, 200));
        assertThat(inputStreams.get("2").getSlice(100)).isEqualTo(testingInput.slice(400, 100));
        assertThat(inputStreams.get("3").getSlice(50)).isEqualTo(testingInput.slice(1100, 50));
        assertThat(inputStreams.get("4").getSlice(600)).isEqualTo(testingInput.slice(2000, 600));
        assertThat(dataSource.getConcurrentReads()).isEqualTo(3);
        // only the second part of "4" is read on the calling thread
        assertThat(dataSource.getReads()).isEqualTo(1);
        assertThat(dataSource.getReadBytes()).isEqualTo(500 + 50 + 600);

        // a stream that has been read is not prefetched again
        inputStreams.get("1").prefetch();
        assertThat(dataSource.getConcurrentReads()).isEqualTo(3);

        // freeing a prefetched range releases its memory
        inputStreams.get("5").prefetch();
        assertThat(dataSource.getConcurrentReads()).isEqualTo(4);
        inputStreams.get("5").close();
        inputStreams.get("1").close();
        inputStreams.get("2").close();
        inputStreams.get("3").close();
        inputStreams.get("4").close();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
    }

    @Test
    public void testPrefetchFailureFallsBackToRead()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                ParquetReaderOptions.builder().build(),
                Optional.of(directExecutor()));
        dataSource.setFailConcurrentReads(true);
        Map<String, ChunkedInputStream> inputStreams = dataSource.planRead(
                ImmutableListMultimap.of("1", new DiskRange(100, 200)),
                newSimpleAggregatedMemoryContext());

        inputStreams.get("1").prefetch();
        assertThat(dataSource.getConcurrentReads()).isEqualTo(1);
        assertThat(inputStreams.get("1").getSlice(200)).isEqualTo(testingInput.slice(100, 200));
        assertThat(dataSource.getReads()).isEqualTo(1);
    }

    @Test
    public void testPrefetchWithoutExecutor()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(testingInput, ParquetReaderOptions.builder().build());
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Map<String, ChunkedInputStream> inputStreams = dataSource.planRead(
                ImmutableListMultimap.of("1", new DiskRange(100, 200)),
                memoryContext);

        inputStreams.get("1").prefetch();
        assertThat(memoryContext.getBytes()).isEqualTo(0);
        assertThat(dataSource.getConcurrentReads()).isEqualTo(0);
        assertThat(inputStreams.get("1").getSlice(200)).isEqualTo(testingInput.slice(100, 200));
        assertThat(dataSource.getReads()).isEqualTo(1);
    }

    private static Slice createTestingInput()
    {
        Slice testingInput = Slices.allocate(4000);
        SliceOutput out = testingInput.getOutput();
        IntStream.range(0, 1000).forEach(out::appendInt);
        return testingInput;
    }
}
