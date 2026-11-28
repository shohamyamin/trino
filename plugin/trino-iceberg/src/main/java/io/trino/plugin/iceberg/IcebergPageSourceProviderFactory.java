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
package io.trino.plugin.iceberg;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.cache.ParquetFooterCache;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.iceberg.encryption.EncryptionManagerFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.BlocksHashFactory;
import io.trino.spi.connector.ConnectorPageSourceProviderFactory;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static java.lang.Math.clamp;
import static java.util.Objects.requireNonNull;

public class IcebergPageSourceProviderFactory
        implements ConnectorPageSourceProviderFactory
{
    private static final int PARQUET_PREFETCH_PARALLELISM = clamp(8L * Runtime.getRuntime().availableProcessors(), 32, 256);

    private final IcebergFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private final ParquetFooterCache parquetFooterCache;
    private final BlocksHashFactory blocksHashFactory;
    private final EncryptionManagerFactory encryptionManagerFactory;
    private final int domainCompactionThreshold;
    private final Executor deleteLoadingExecutor;
    private final Executor parquetPrefetchExecutor;

    @Inject
    public IcebergPageSourceProviderFactory(
            IcebergFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager,
            ParquetFooterCache parquetFooterCache,
            BlocksHashFactory blocksHashFactory,
            IcebergConfig config,
            EncryptionManagerFactory encryptionManagerFactory,
            @ForIcebergDeleteLoading ExecutorService deleteLoadingExecutor,
            @ForIcebergParquetPrefetch ExecutorService parquetPrefetchExecutor)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = orcReaderConfig.toOrcReaderOptions();
        this.parquetReaderOptions = parquetReaderConfig.toParquetReaderOptions();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.parquetFooterCache = requireNonNull(parquetFooterCache, "parquetFooterCache is null");
        this.blocksHashFactory = requireNonNull(blocksHashFactory, "blocksHashFactory is null");
        this.encryptionManagerFactory = requireNonNull(encryptionManagerFactory, "encryptionManagerFactory is null");
        this.domainCompactionThreshold = config.getDomainCompactionThreshold();
        requireNonNull(deleteLoadingExecutor, "deleteLoadingExecutor is null");
        if (config.getDeleteLoadingParallelism() == 0) {
            this.deleteLoadingExecutor = deleteLoadingExecutor;
        }
        else {
            this.deleteLoadingExecutor = new BoundedExecutor(deleteLoadingExecutor, config.getDeleteLoadingParallelism());
        }
        // bound the number of concurrent background reads, which each hold an object store connection
        this.parquetPrefetchExecutor = new BoundedExecutor(requireNonNull(parquetPrefetchExecutor, "parquetPrefetchExecutor is null"), PARQUET_PREFETCH_PARALLELISM);
    }

    @Override
    public IcebergPageSourceProvider createPageSourceProvider(MemoryContext memoryContext)
    {
        return new IcebergPageSourceProvider(fileSystemFactory, fileIoFactory, fileFormatDataSourceStats, orcReaderOptions, parquetReaderOptions, typeManager, parquetFooterCache, blocksHashFactory, encryptionManagerFactory, memoryContext, domainCompactionThreshold, deleteLoadingExecutor, Optional.of(parquetPrefetchExecutor));
    }
}
