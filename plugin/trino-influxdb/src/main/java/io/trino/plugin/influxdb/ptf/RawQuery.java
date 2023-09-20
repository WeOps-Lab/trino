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
package io.trino.plugin.influxdb.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.influxdb.InfluxColumnHandle;
import io.trino.plugin.influxdb.InfluxMetadata;
import io.trino.plugin.influxdb.InfluxTableHandle;
import io.trino.spi.connector.*;

import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import com.google.inject.Inject;
import com.google.inject.Provider;

public class RawQuery
        implements Provider<ConnectorTableFunction> {
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "raw_query";

    private final InfluxMetadata metadata;

    @Inject
    public RawQuery(InfluxMetadata metadata) {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new RawQueryFunction(metadata);
    }

    public static class RawQueryFunction
            extends AbstractConnectorTableFunction {
        private final InfluxMetadata metadata;

        public RawQueryFunction(InfluxMetadata metadata) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("SCHEMA")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("INDEX")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("BK_DATA_ID")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("METRIC")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("FUNCTION")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("START_TIME")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("END_TIME")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("GROUP_BY")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("QUERY")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(ConnectorSession session, ConnectorTransactionHandle transaction, Map<String, Argument> arguments, ConnectorAccessControl accessControl) {
            String index = getStringArgument(arguments, "INDEX");
            String schema = getStringArgument(arguments, "SCHEMA");
            String query = getStringArgument(arguments, "QUERY");

            String bk_data_id = getStringArgument(arguments, "BK_DATA_ID");
            String start_time = getStringArgument(arguments, "START_TIME");
            String end_time = getStringArgument(arguments, "END_TIME");
            String metric = getStringArgument(arguments, "METRIC");
            String function = getStringArgument(arguments, "FUNCTION");
            String group_by = getStringArgument(arguments, "GROUP_BY");
            boolean calFunction = function.equals("mean") || function.equals("min") || function.equals("max");

            if (!calFunction){
                return TableFunctionAnalysis.builder().build();
            }

            // 默认7天
            if (start_time.isEmpty() || end_time.isEmpty()) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");

                // 格式化时间为所需的格式
                start_time = LocalDateTime.now().minusDays(7).format(formatter);
                end_time = LocalDateTime.now().format(formatter);
            }

            // 主机性能指标
            if (bk_data_id.equals("host")) {
                schema = "system";
                if (metric.equals("cpu_info")) {
                    metric = "usage";
                    index = "cpu_summary";
                } else if (metric.equals("memory_info")) {
                    metric = "pct_used";
                    index = "mem";
                }

                query += String.format("""
                    SELECT
                        %s(%s) as %s
                    FROM
                        %s
                    WHERE
                      time>'%s'
                      AND time<'%s'\s
                      """, function, metric, metric, index, start_time, end_time);

                if (group_by.isEmpty()) {
                    group_by += "hostname, ip, bk_cloud_id, bk_biz_id";
                }else {
                    group_by += " ,hostname, ip, bk_cloud_id, bk_biz_id";
                }
            }else {
                query += String.format("""
                    SELECT
                        time, metric_name, %s(metric_value) as metric_value
                    FROM
                        %s
                    WHERE
                      time>'%s'
                      AND time<'%s'
                      AND metric_name='%s'\s
                      """, function, index, start_time, end_time, metric);
            }



            if (!group_by.isEmpty()) {
                query += String.format("""
                        GROUP BY
                            %s
                        """, group_by);
            }

            InfluxTableHandle tableHandle = new InfluxTableHandle(schema, index, ImmutableList.of(), Optional.of(query));
            ConnectorTableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            Map<String, ColumnHandle> columnsByName = metadata.getColumnHandles(session, tableHandle);
            List<ColumnHandle> columns = tableSchema.getColumns().stream()
                    .map(ColumnSchema::getName)
                    .map(columnsByName::get)
                    .collect(toImmutableList());

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(InfluxColumnHandle.class::cast)
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toList()));

            RawQueryFunctionHandle handle = new RawQueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }

        private static String getStringArgument(Map<String, Argument> arguments, String argName) {
            Slice slice = ((Slice) ((ScalarArgument) arguments.get(argName)).getValue());
            return slice != null ? slice.toStringUtf8() : "";
        }

    }

    public static class RawQueryFunctionHandle
            implements ConnectorTableFunctionHandle {
        private final InfluxTableHandle tableHandle;

        @JsonCreator
        public RawQueryFunctionHandle(@JsonProperty("tableHandle") InfluxTableHandle tableHandle) {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }
    }
}
