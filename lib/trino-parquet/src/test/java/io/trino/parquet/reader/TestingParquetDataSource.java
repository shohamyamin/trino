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

import io.airlift.slice.Slice;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.toIntExact;

public class TestingParquetDataSource
        extends AbstractParquetDataSource
{
    private final Slice input;
    private final Optional<Executor> prefetchExecutor;
    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicInteger concurrentReads = new AtomicInteger();
    private volatile boolean failConcurrentReads;

    public TestingParquetDataSource(Slice slice, ParquetReaderOptions options)
            throws IOException
    {
        this(slice, options, Optional.empty());
    }

    public TestingParquetDataSource(Slice slice, ParquetReaderOptions options, Optional<Executor> prefetchExecutor)
            throws IOException
    {
        super(new ParquetDataSourceId(UUID.randomUUID().toString()), slice.length(), options);
        this.input = slice;
        this.prefetchExecutor = prefetchExecutor;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        reads.incrementAndGet();
        input.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
    }

    @Override
    protected Optional<Executor> getPrefetchExecutor()
    {
        return prefetchExecutor;
    }

    @Override
    protected void readInternalConcurrently(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        concurrentReads.incrementAndGet();
        if (failConcurrentReads) {
            throw new IOException("test failure");
        }
        input.getBytes(toIntExact(position), buffer, bufferOffset, bufferLength);
    }

    public int getReads()
    {
        return reads.get();
    }

    public int getConcurrentReads()
    {
        return concurrentReads.get();
    }

    public void setFailConcurrentReads(boolean failConcurrentReads)
    {
        this.failConcurrentReads = failConcurrentReads;
    }
}
