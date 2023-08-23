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
package io.trino.plugin.elasticsearch.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.elasticsearch.ElasticsearchColumnHandle;
import io.trino.plugin.elasticsearch.ElasticsearchMetadata;
import io.trino.plugin.elasticsearch.ElasticsearchTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.*;

import java.util.*;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class AlarmInfo
        implements Provider<ConnectorTableFunction> {
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "alarm_info";

    private final ElasticsearchMetadata metadata;

    @Inject
    public AlarmInfo(ElasticsearchMetadata metadata) {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new AlarmInfoFunction(metadata);
    }

    public static class AlarmInfoFunction
            extends AbstractConnectorTableFunction {
        private final ElasticsearchMetadata metadata;

        public AlarmInfoFunction(ElasticsearchMetadata metadata) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("SCHEMA")
                                    .type(VARCHAR)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("INDEX")
                                    .type(VARCHAR)
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
                                    .name("BK_OBJ_ID")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ALARM_LEVEL")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("ALARM_STATUS")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("BK_BIZ_ID")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("SIZE")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("FIELDS")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("SORT")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build()

                    ),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        public static String parseSortString(String sortString) {
            StringBuilder stringBuilder = new StringBuilder();
            String[] sortFields = sortString.split(",");
            stringBuilder.append("\"sort\": [");
            for (int i = 0; i < sortFields.length; i++) {
                if (i > 0) {
                    stringBuilder.append(",");
                }
                String sortField = sortFields[i];
                if (sortField.startsWith("-")) {
                    sortField = sortField.substring(1);
                    stringBuilder.append("{\"").append(sortField).append("\":{\"order\":\"desc\"}}");
                } else {
                    stringBuilder.append("{\"").append(sortField).append("\":{\"order\":\"asc\"}}");
                }
            }
            stringBuilder.append("]");
            return stringBuilder.toString();
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {
            String schema = ((Slice) ((ScalarArgument) arguments.get("SCHEMA")).getValue()).toStringUtf8();
            String index = ((Slice) ((ScalarArgument) arguments.get("INDEX")).getValue()).toStringUtf8();
            Slice startTimeSlice = ((Slice) ((ScalarArgument) arguments.get("START_TIME")).getValue());
            String startTime = startTimeSlice != null ? startTimeSlice.toStringUtf8() : null;
            Slice endTimeSlice = ((Slice) ((ScalarArgument) arguments.get("END_TIME")).getValue());
            String endTime = endTimeSlice != null ? endTimeSlice.toStringUtf8() : null;
            Slice instObjSlice = ((Slice) ((ScalarArgument) arguments.get("BK_OBJ_ID")).getValue());
            String instObj = instObjSlice != null ? instObjSlice.toStringUtf8() : null;
            Slice instBizSlice = ((Slice) ((ScalarArgument) arguments.get("BK_BIZ_ID")).getValue());
            String instBiz = instBizSlice != null ? instBizSlice.toStringUtf8() : null;
            Slice alarmLevelSlice = ((Slice) ((ScalarArgument) arguments.get("ALARM_LEVEL")).getValue());
            String alarmLevel = alarmLevelSlice != null ? alarmLevelSlice.toStringUtf8() : null;
            Slice alarmStatusSlice = ((Slice) ((ScalarArgument) arguments.get("ALARM_STATUS")).getValue());
            String alarmStatus = alarmStatusSlice != null ? alarmStatusSlice.toStringUtf8() : null;
            Slice SizeSlice = ((Slice) ((ScalarArgument) arguments.get("SIZE")).getValue());
            String size = SizeSlice != null ? SizeSlice.toStringUtf8() : null;
            Slice fieldsSlice = ((Slice) ((ScalarArgument) arguments.get("FIELDS")).getValue());
            String fields = fieldsSlice != null ? fieldsSlice.toStringUtf8() : null;
            Slice sortSlice = ((Slice) ((ScalarArgument) arguments.get("SORT")).getValue());
            String sort = sortSlice != null ? sortSlice.toStringUtf8() : null;
            StringBuilder dateFilter = new StringBuilder("");
            StringBuilder levelFilter = new StringBuilder("");
            StringBuilder statusFilter = new StringBuilder("");
            StringBuilder bizFilter = new StringBuilder("");
            StringBuilder objFilter = new StringBuilder("");

            if (startTime != null && endTime != null) {
                dateFilter.append(String.format(
                        """
                                  {
                                    "range": {
                                      "alarm_time": {
                                        "gte": "%s",
                                        "lte": "%s"
                                      }
                                    }
                                  }
                                """
                        , startTime, endTime));
            } else if (startTime != null) {
                dateFilter.append(String.format(
                        """
                                  {
                                    "range": {
                                      "alarm_time": {
                                        "gte": "%s"
                                      }
                                    }
                                  }
                                """
                        , startTime));
            } else if (endTime != null) {
                dateFilter.append(String.format(
                        """
                                  {
                                    "range": {
                                      "alarm_time": {
                                        "lte": "%s"
                                      }
                                    }
                                  }
                                """
                        , endTime));
            }
            ;
            if (instObj != null) {
                objFilter.append(String.format("""
                        { "term": {
                            "bk_obj_id": "%s"
                            }
                        }
                        """, instObj));
            }

            if (instBiz != null) {
                bizFilter.append(String.format("""
                        { "term": {
                            "bk_biz_id": "%s"
                            }
                        }
                        """, instBiz));
            }
            ;
            if (alarmLevel != null) {
                String[] arr = alarmLevel.split(",");
                String arrList = String.format("[\"%s\"]", String.join("\",\"", arr));
                levelFilter.append(String.format("""
                        { "terms": {
                            "level":%s
                            }
                        }
                        """, arrList));
            }
            ;

            if (alarmStatus != null) {
                String[] arr = alarmStatus.split(",");
                String arr_list = String.format("[\"%s\"]", String.join("\",\"", arr));
                statusFilter.append(String.format("""
                        { "terms": {
                            "status":%s
                            }
                        }
                        """, arr_list));
            }
            ;
            StringBuilder sortString = new StringBuilder("");
            StringBuilder sizeString = new StringBuilder("");
            StringBuilder fieldsString = new StringBuilder("");
            if (sort != null) {
                sortString.append(parseSortString(sort));
            }
            ;
            if (size != null) {
                sizeString.append(String.format("""
                        "size":%s
                        """, size));
            }
            ;
            if (fields != null) {
                String[] arr = fields.split(",");
                String arr_list = String.format("[\"%s\"]", String.join("\",\"", arr));
                fieldsString.append(String.format("""
                         "_source":%s
                        """, arr_list));
            }
            ;
            String[] allFilterArr = Stream.of(dateFilter.toString(), bizFilter.toString(), objFilter.toString(), statusFilter.toString(), levelFilter.toString())
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);
            StringBuilder allFilterString = new StringBuilder("");
            String allFilter = String.join(",", allFilterArr);
            if (!allFilter.isEmpty()) {
                allFilterString.append(String.format("""


                                              "query": {
                                                "bool": {
                                                  "must": [
                                                    %s
                                                  ]
                                                }
                                              }


                        """, allFilter));
            }
            String[] allArr = Stream.of(allFilterString.toString(), fieldsString.toString(), sizeString.toString(), sortString.toString(), "\"track_total_hits\": true")
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);
            String query = String.format("{%s}", String.join(",", allArr));

            ElasticsearchTableHandle tableHandle = new ElasticsearchTableHandle(QUERY, schema, index, Optional.of(query));
            ConnectorTableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            Map<String, ColumnHandle> columnsByName = metadata.getColumnHandles(session, tableHandle);
            List<ColumnHandle> columns = tableSchema.getColumns().stream()
                    .map(ColumnSchema::getName)
                    .map(columnsByName::get)
                    .collect(toImmutableList());

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(ElasticsearchColumnHandle.class::cast)
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toList()));

            AlarmInfoFunctionHandle handle = new AlarmInfoFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class AlarmInfoFunctionHandle
            implements ConnectorTableFunctionHandle {
        private final ElasticsearchTableHandle tableHandle;

        @JsonCreator
        public AlarmInfoFunctionHandle(@JsonProperty("tableHandle") ElasticsearchTableHandle tableHandle) {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }
    }
}
