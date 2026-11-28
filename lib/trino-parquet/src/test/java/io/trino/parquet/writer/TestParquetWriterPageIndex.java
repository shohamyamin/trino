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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.TestingParquetDataSource;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveComparator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongPredicate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTestUtils.createParquetReader;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetWriterPageIndex
{
    private static final int ROWS = 50_000;
    private static final int INPUT_PAGE_ROWS = 777;
    private static final int PAGE_VALUE_COUNT = 1000;
    private static final int FIRST_NON_NULL_B = 5_000;
    private static final String C_SUFFIX = "c".repeat(100);

    private static final ArrayType E_TYPE = new ArrayType(BIGINT);
    private static final RowType F_TYPE = rowType(field("x", BIGINT));
    private static final List<String> NAMES = ImmutableList.of("a", "b", "c", "d", "e", "f");
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, INTEGER, VARCHAR, DOUBLE, E_TYPE, F_TYPE);

    @Test
    public void testPageIndexesReadByParquetMr()
            throws IOException
    {
        Path file = writeFile(true);
        try {
            try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
                List<BlockMetaData> blocks = reader.getFooter().getBlocks();
                assertThat(blocks).hasSize(1);
                BlockMetaData block = blocks.getFirst();
                for (ColumnChunkMetaData column : block.getColumns()) {
                    String name = column.getPath().toArray()[0];
                    OffsetIndex offsetIndex = reader.readOffsetIndex(column);
                    assertThat(offsetIndex).as(name).isNotNull();
                    assertThat(offsetIndex.getPageCount()).as(name).isGreaterThan(10);
                    // pages are contiguous, start at the first data page and end with the column chunk
                    assertThat(offsetIndex.getOffset(0)).isEqualTo(column.getFirstDataPageOffset());
                    assertThat(offsetIndex.getFirstRowIndex(0)).isZero();
                    for (int page = 1; page < offsetIndex.getPageCount(); page++) {
                        assertThat(offsetIndex.getOffset(page)).isEqualTo(offsetIndex.getOffset(page - 1) + offsetIndex.getCompressedPageSize(page - 1));
                        assertThat(offsetIndex.getFirstRowIndex(page)).isGreaterThan(offsetIndex.getFirstRowIndex(page - 1));
                    }
                    int lastPage = offsetIndex.getPageCount() - 1;
                    assertThat(offsetIndex.getOffset(lastPage) + offsetIndex.getCompressedPageSize(lastPage)).isEqualTo(column.getStartingPos() + column.getTotalSize());
                    assertThat(offsetIndex.getFirstRowIndex(lastPage)).isLessThan(block.getRowCount());

                    ColumnIndex columnIndex = reader.readColumnIndex(column);
                    if (name.equals("d")) {
                        // no page bounds for floating point columns
                        assertThat(columnIndex).isNull();
                        continue;
                    }
                    assertThat(columnIndex).as(name).isNotNull();
                    assertThat(columnIndex.getNullPages()).hasSize(offsetIndex.getPageCount());
                    switch (name) {
                        case "a" -> verifyBigintColumnIndex(offsetIndex, columnIndex);
                        case "b" -> verifyNullableColumnIndex(offsetIndex, columnIndex);
                        case "c" -> verifyTruncatedColumnIndex(offsetIndex, columnIndex, column.getPrimitiveType().comparator());
                        default -> {}
                    }
                }
            }

            // parquet-mr reads only the pages that can contain a = 30000, and the rows of all columns stay aligned
            int rows = 0;
            boolean found = false;
            try (org.apache.parquet.hadoop.ParquetReader<Group> reader = org.apache.parquet.hadoop.ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toUri()))
                    .withConf(new Configuration(false))
                    .withFilter(FilterCompat.get(eq(longColumn("a"), 30_000L)))
                    .useColumnIndexFilter(true)
                    .useRecordFilter(false)
                    .useStatsFilter(false)
                    .useDictionaryFilter(false)
                    .build()) {
                for (Group group = reader.read(); group != null; group = reader.read()) {
                    rows++;
                    long a = group.getLong("a", 0);
                    found |= a == 30_000;
                    verifyRow(a, parquetMrRow(group));
                }
            }
            assertThat(found).isTrue();
            assertThat(rows).isBetween(1, 2 * PAGE_VALUE_COUNT);
        }
        finally {
            Files.delete(file);
        }
    }

    @Test
    public void testTrinoReaderSkipsPages()
            throws IOException
    {
        Path file = writeFile(true);
        try {
            ParquetDataSource dataSource = new TestingParquetDataSource(wrappedBuffer(Files.readAllBytes(file)), ParquetReaderOptions.defaultOptions());
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, Optional.empty());
            for (BlockMetadata block : parquetMetadata.getBlocks()) {
                for (ColumnChunkMetadata column : block.columns()) {
                    assertThat(column.getOffsetIndexReference()).isNotNull();
                }
            }

            assertFilteredRead(dataSource, parquetMetadata, "a", Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 30_000L, true, 30_010L, true)), false), a -> a >= 30_000 && a <= 30_010, 2 * PAGE_VALUE_COUNT);
            assertFilteredRead(dataSource, parquetMetadata, "c", Domain.singleValue(VARCHAR, utf8Slice(cValue(12_345))), a -> a == 12_345, 2 * PAGE_VALUE_COUNT);
            // b has only nulls in the first pages, which are skipped, and values 0-999 in every later page
            assertFilteredRead(dataSource, parquetMetadata, "b", Domain.singleValue(INTEGER, 3L), a -> a >= FIRST_NON_NULL_B && bValue(a) == 3, ROWS - FIRST_NON_NULL_B + PAGE_VALUE_COUNT);
        }
        finally {
            Files.delete(file);
        }
    }

    @Test
    public void testPageIndexDisabled()
            throws IOException
    {
        Path file = writeFile(false);
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData column : block.getColumns()) {
                    assertThat(reader.readOffsetIndex(column)).isNull();
                    assertThat(reader.readColumnIndex(column)).isNull();
                }
            }
        }
        finally {
            Files.delete(file);
        }
    }

    private static void assertFilteredRead(ParquetDataSource dataSource, ParquetMetadata parquetMetadata, String column, Domain domain, LongPredicate matches, int maxRowsRead)
            throws IOException
    {
        Set<Long> seen = new HashSet<>();
        try (ParquetReader reader = createParquetReader(
                dataSource,
                parquetMetadata,
                ParquetReaderOptions.defaultOptions(),
                newSimpleAggregatedMemoryContext(),
                TYPES,
                NAMES,
                TupleDomain.withColumnDomains(Map.of(column, domain)))) {
            for (SourcePage page = reader.nextPage(); page != null; page = reader.nextPage()) {
                for (int position = 0; position < page.getPositionCount(); position++) {
                    List<Object> row = new ArrayList<>();
                    for (int channel = 0; channel < TYPES.size(); channel++) {
                        row.add(TYPES.get(channel).getObjectValue(page.getBlock(channel), position));
                    }
                    long a = (Long) row.getFirst();
                    seen.add(a);
                    verifyRow(a, row);
                }
            }
        }
        // all matching rows are read, pages that cannot match are skipped
        for (long a = 0; a < ROWS; a++) {
            if (matches.test(a)) {
                assertThat(seen).contains(a);
            }
        }
        assertThat(seen.size()).isLessThanOrEqualTo(maxRowsRead);
    }

    private static void verifyBigintColumnIndex(OffsetIndex offsetIndex, ColumnIndex columnIndex)
    {
        for (int page = 0; page < offsetIndex.getPageCount(); page++) {
            long firstRow = offsetIndex.getFirstRowIndex(page);
            long lastRow = page + 1 < offsetIndex.getPageCount() ? offsetIndex.getFirstRowIndex(page + 1) - 1 : ROWS - 1;
            assertThat(columnIndex.getNullPages().get(page)).isFalse();
            assertThat(columnIndex.getNullCounts().get(page)).isZero();
            assertThat(toLong(columnIndex.getMinValues().get(page))).isEqualTo(firstRow);
            assertThat(toLong(columnIndex.getMaxValues().get(page))).isEqualTo(lastRow);
        }
    }

    private static void verifyNullableColumnIndex(OffsetIndex offsetIndex, ColumnIndex columnIndex)
    {
        for (int page = 0; page < offsetIndex.getPageCount(); page++) {
            long firstRow = offsetIndex.getFirstRowIndex(page);
            long lastRow = page + 1 < offsetIndex.getPageCount() ? offsetIndex.getFirstRowIndex(page + 1) - 1 : ROWS - 1;
            long nulls = Math.max(0, Math.min(lastRow + 1, FIRST_NON_NULL_B) - firstRow);
            assertThat(columnIndex.getNullCounts().get(page)).isEqualTo(nulls);
            assertThat(columnIndex.getNullPages().get(page)).isEqualTo(lastRow < FIRST_NON_NULL_B);
            if (lastRow >= FIRST_NON_NULL_B) {
                long min = Long.MAX_VALUE;
                long max = Long.MIN_VALUE;
                for (long row = Math.max(firstRow, FIRST_NON_NULL_B); row <= lastRow; row++) {
                    min = Math.min(min, bValue(row));
                    max = Math.max(max, bValue(row));
                }
                assertThat(toInt(columnIndex.getMinValues().get(page))).isEqualTo(min);
                assertThat(toInt(columnIndex.getMaxValues().get(page))).isEqualTo(max);
            }
        }
    }

    private static void verifyTruncatedColumnIndex(OffsetIndex offsetIndex, ColumnIndex columnIndex, PrimitiveComparator<Binary> comparator)
    {
        for (int page = 0; page < offsetIndex.getPageCount(); page++) {
            long firstRow = offsetIndex.getFirstRowIndex(page);
            long lastRow = page + 1 < offsetIndex.getPageCount() ? offsetIndex.getFirstRowIndex(page + 1) - 1 : ROWS - 1;
            Binary min = Binary.fromConstantByteBuffer(columnIndex.getMinValues().get(page));
            Binary max = Binary.fromConstantByteBuffer(columnIndex.getMaxValues().get(page));
            // the values are longer than the truncation length; the bounds must still contain all values of the page
            assertThat(min.length()).isLessThanOrEqualTo(64);
            assertThat(max.length()).isLessThanOrEqualTo(64);
            assertThat(comparator.compare(min, Binary.fromString(cValue(firstRow)))).isLessThanOrEqualTo(0);
            assertThat(comparator.compare(max, Binary.fromString(cValue(lastRow)))).isGreaterThanOrEqualTo(0);
        }
    }

    private static List<Object> parquetMrRow(Group group)
    {
        List<Object> row = new ArrayList<>();
        row.add(group.getLong("a", 0));
        row.add(group.getFieldRepetitionCount("b") == 0 ? null : group.getInteger("b", 0));
        row.add(group.getString("c", 0));
        row.add(group.getDouble("d", 0));
        if (group.getFieldRepetitionCount("e") == 0) {
            row.add(null);
        }
        else {
            Group list = group.getGroup("e", 0);
            List<Long> elements = new ArrayList<>();
            for (int i = 0; i < list.getFieldRepetitionCount(0); i++) {
                Group element = list.getGroup(0, i);
                elements.add(element.getFieldRepetitionCount(0) == 0 ? null : element.getLong(0, 0));
            }
            row.add(elements);
        }
        if (group.getFieldRepetitionCount("f") == 0) {
            row.add(null);
        }
        else {
            Group struct = group.getGroup("f", 0);
            row.add(ImmutableList.of(struct.getLong(0, 0)));
        }
        return row;
    }

    private static void verifyRow(long a, List<Object> row)
    {
        assertThat(row.get(0)).isEqualTo(a);
        assertThat(row.get(1)).isEqualTo(a < FIRST_NON_NULL_B ? null : (int) bValue(a));
        assertThat(row.get(2)).isEqualTo(cValue(a));
        assertThat(row.get(3)).isEqualTo(a / 2.0);
        assertThat(row.get(4)).isEqualTo(eValue(a));
        if (a % 7 == 0) {
            assertThat(row.get(5)).isNull();
        }
        else {
            assertThat(row.get(5)).isEqualTo(ImmutableList.of(a * 2));
        }
    }

    private static Path writeFile(boolean writePageIndex)
            throws IOException
    {
        List<Page> pages = new ArrayList<>();
        for (int start = 0; start < ROWS; start += INPUT_PAGE_ROWS) {
            pages.add(createPage(start, Math.min(ROWS, start + INPUT_PAGE_ROWS)));
        }
        Slice data = writeParquetFile(
                ParquetWriterOptions.builder()
                        .setMaxPageValueCount(PAGE_VALUE_COUNT)
                        .setWritePageIndex(writePageIndex)
                        .build(),
                TYPES,
                NAMES,
                pages);
        Path file = Files.createTempFile("page-index", ".parquet");
        Files.write(file, data.getBytes());
        return file;
    }

    private static Page createPage(int start, int end)
    {
        BlockBuilder a = BIGINT.createBlockBuilder(null, end - start);
        BlockBuilder b = INTEGER.createBlockBuilder(null, end - start);
        BlockBuilder c = VARCHAR.createBlockBuilder(null, end - start);
        BlockBuilder d = DOUBLE.createBlockBuilder(null, end - start);
        ArrayBlockBuilder e = E_TYPE.createBlockBuilder(null, end - start);
        RowBlockBuilder f = F_TYPE.createBlockBuilder(null, end - start);
        for (long row = start; row < end; row++) {
            BIGINT.writeLong(a, row);
            if (row < FIRST_NON_NULL_B) {
                b.appendNull();
            }
            else {
                INTEGER.writeLong(b, bValue(row));
            }
            VARCHAR.writeSlice(c, utf8Slice(cValue(row)));
            DOUBLE.writeDouble(d, row / 2.0);
            List<Long> elements = eValue(row);
            if (elements == null) {
                e.appendNull();
            }
            else {
                e.buildEntry(elementBuilder -> {
                    for (Long element : elements) {
                        if (element == null) {
                            elementBuilder.appendNull();
                        }
                        else {
                            BIGINT.writeLong(elementBuilder, element);
                        }
                    }
                });
            }
            if (row % 7 == 0) {
                f.appendNull();
            }
            else {
                long x = row * 2;
                f.buildEntry(fieldBuilders -> BIGINT.writeLong(fieldBuilders.getFirst(), x));
            }
        }
        Block[] blocks = {a.build(), b.build(), c.build(), d.build(), e.build(), f.build()};
        return new Page(blocks);
    }

    private static long bValue(long row)
    {
        return (row * 7) % 1000;
    }

    private static String cValue(long row)
    {
        // longer than the truncation length of page bounds, and sorted
        return "%08d".formatted(row) + C_SUFFIX;
    }

    // a varying number of elements per row, so pages of the element column hold different numbers of rows
    private static List<Long> eValue(long row)
    {
        if (row % 5 == 0) {
            return null;
        }
        List<Long> elements = new ArrayList<>();
        for (int i = 0; i < row % 4; i++) {
            elements.add(i == 2 ? null : row + i);
        }
        return elements;
    }

    private static long toLong(ByteBuffer buffer)
    {
        return buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).getLong(0);
    }

    private static long toInt(ByteBuffer buffer)
    {
        return buffer.duplicate().order(ByteOrder.LITTLE_ENDIAN).getInt(0);
    }
}
