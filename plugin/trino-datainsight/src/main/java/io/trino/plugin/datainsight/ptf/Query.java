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
package io.trino.plugin.datainsight.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.datainsight.*;
import io.trino.plugin.datainsight.DatainsightTableHandle.Type;

import io.trino.spi.connector.*;
import io.trino.spi.function.table.*;

import java.util.*;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class Query
        implements Provider<ConnectorTableFunction> {
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "query";

    private final DatainsightMetadata metadata;

    @Inject
    public Query(DatainsightMetadata metadata) {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new QueryFunction(metadata);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction {
        private final DatainsightMetadata metadata;

        public QueryFunction(DatainsightMetadata metadata) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder()
                                    .name("STREAMS")
                                    .type(VARCHAR)
                                    .build()
                            ,
                            ScalarArgumentSpecification.builder()
                                    .name("TYPE")
                                    .type(VARCHAR)
                                    .build()
                            ,
                            ScalarArgumentSpecification.builder()
                                    .name("QUERY")
                                    .type(VARCHAR)
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {

            String streams = ((Slice) ((ScalarArgument) arguments.get("STREAMS")).getValue()).toStringUtf8();
            String query = ((Slice) ((ScalarArgument) arguments.get("QUERY")).getValue()).toStringUtf8();
            String type = ((Slice) ((ScalarArgument) arguments.get("TYPE")).getValue()).toStringUtf8();
            Type sType = Type.valueOf(type.toUpperCase(Locale.ENGLISH));
            String index = metadata.getIndexesByMongo(Optional.of(streams));
            query = query.replace("$index", index);
            DatainsightTableHandle tableHandle = new DatainsightTableHandle(sType, SCHEMA_NAME, index, Optional.of(query));
            Map<String, ColumnHandle> columnsByName =metadata.getColumnHandles(session, tableHandle);
            // 构建columns List,按照columnHandle type的顺序
            List<ColumnHandle> columns = columnsByName.values().stream().sorted(Comparator.comparing(column -> ((DatainsightColumnHandle) column).getIndex())).collect(toImmutableList());

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(DatainsightColumnHandle.class::cast)
                    .map(column -> new Descriptor.Field(column.getName(), Optional.of(column.getType())))
                    .collect(toList()));

            QueryFunctionHandle handle = new QueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    public static class QueryFunctionHandle
            implements ConnectorTableFunctionHandle {
        private final DatainsightTableHandle tableHandle;

        @JsonCreator
        public QueryFunctionHandle(@JsonProperty("tableHandle") DatainsightTableHandle tableHandle) {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle() {
            return tableHandle;
        }
    }
}
