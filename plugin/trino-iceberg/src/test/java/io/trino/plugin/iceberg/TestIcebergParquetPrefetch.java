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

import com.google.common.collect.ImmutableMultiset;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergParquetPrefetch
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.file-format", "PARQUET")
                // read column chunks separately
                .addIcebergProperty("parquet.max-buffer-size", "1MB")
                .addIcebergProperty("parquet.max-merge-distance", "0B")
                .disableSchemaInitializer()
                .build();
    }

    @BeforeAll
    void setUp()
    {
        Session writeSession = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, "parquet_writer_row_group_max_row_count", "7000")
                .build();
        assertUpdate("CREATE SCHEMA tpch");
        assertUpdate(writeSession, "CREATE TABLE tpch.lineitem AS SELECT * FROM tpch.tiny.lineitem", 60175);
        assertUpdate(writeSession, "CREATE TABLE tpch.orders AS SELECT * FROM tpch.tiny.orders", 15000);
    }

    @Test
    void testReadsWithPrefetch()
    {
        // files are read through the data source that loads column chunks in the background
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, "parquet_small_file_threshold", "0B")
                .build();

        assertSameResults(session, "SELECT * FROM tpch.lineitem");
        assertSameResults(session, "SELECT orderkey, linenumber, comment, extendedprice FROM tpch.lineitem WHERE shipmode = 'AIR' AND quantity > 30");
        assertSameResults(session, "SELECT returnflag, linestatus, sum(quantity), sum(extendedprice * (1 - discount)), count(*) FROM tpch.lineitem GROUP BY 1, 2");
        // a selective filter, where most column chunks that are loaded in the background are never used
        assertSameResults(session, "SELECT comment FROM tpch.lineitem WHERE orderkey = 7");
        assertSameResults(session, "SELECT o.orderpriority, count(*) FROM tpch.orders o JOIN tpch.lineitem l ON o.orderkey = l.orderkey WHERE l.commitdate < l.receiptdate GROUP BY 1");
        assertThat(query(session, "SELECT * FROM tpch.lineitem LIMIT 10"))
                .result().rowCount().isEqualTo(10);
    }

    private void assertSameResults(Session session, String sql)
    {
        String expectedSql = sql.replace("tpch.lineitem", "tpch.tiny.lineitem").replace("tpch.orders", "tpch.tiny.orders");
        assertThat(ImmutableMultiset.copyOf(computeActual(session, sql).getMaterializedRows()))
                .isEqualTo(ImmutableMultiset.copyOf(computeActual(expectedSql).getMaterializedRows()));
    }
}
