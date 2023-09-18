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
package io.trino.plugin.mongodb.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.mongodb.*;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.*;
import org.bson.Document;
import org.bson.json.JsonParseException;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class QueryCMDB
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "get_inst";

    private final MongoMetadata metadata;
    private final MongoSession session;

    @Inject
    public QueryCMDB(MongoSession session)
    {
        requireNonNull(session, "session is null");
        this.metadata = new MongoMetadata(session);
        this.session = session;
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new QueryFunction(metadata, session);
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final MongoMetadata metadata;
        private final MongoSession mongoSession;

        public QueryFunction(MongoMetadata metadata, MongoSession mongoSession) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                    .name("DATABASE")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("FILTER")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("BK_OBJ_ID")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("FIELDS")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("REGEX")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build(),
                            ScalarArgumentSpecification.builder()
                                    .name("BIZ")
                                    .type(VARCHAR)
                                    .defaultValue(null)
                                    .build()),
                    GENERIC_TABLE);
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {

            String database =getStringArgument(arguments, "DATABASE");
            String biz = getStringArgument(arguments, "BIZ");
            String objectName = getStringArgument(arguments, "BK_OBJ_ID");
            String filter = getStringArgument(arguments, "FILTER");
            String fields = getStringArgument(arguments, "FIELDS");
            String regex = getStringArgument(arguments, "REGEX");
            String collection = "";

            database = database.isEmpty() ? "cmdb" : database;

            if ((filter.isEmpty())&&!fields.isEmpty() && !regex.isEmpty()) {
                filter = regexFilter(fields, regex);
            }

            filter = filter.isEmpty() ? "{}" : filter;

            if (!biz.isEmpty()) {
                collection = "cc_applicationbase";
            } else {
                if (!objectName.isEmpty()) {
                    // 主机host对象需要特殊处理
                    if (objectName.equals("host")) {
                        collection = "cc_hostbase";
                    } else {
                        collection = "cc_objectbase_0_pub_" + objectName;
                    }
                }
            }

            if (!database.equals(database.toLowerCase(ENGLISH))) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Only lowercase database name is supported");
            }
            if (!collection.equals(collection.toLowerCase(ENGLISH))) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Only lowercase collection name is supported");
            }
            RemoteTableName remoteTableName = mongoSession.toRemoteSchemaTableName(new SchemaTableName(database, collection));
            // Don't store Document object to MongoTableHandle for avoiding serialization issue
            parseFilter(filter);

            MongoTableHandle tableHandle = new MongoTableHandle(new SchemaTableName(database, collection), remoteTableName, Optional.of(filter));
            ConnectorTableSchema tableSchema = metadata.getTableSchema(session, tableHandle);
            Map<String, ColumnHandle> columnsByName = metadata.getColumnHandles(session, tableHandle);
            List<ColumnHandle> columns = tableSchema.getColumns().stream()
                    .filter(column -> !column.isHidden())
                    .map(ColumnSchema::getName)
                    .map(columnsByName::get)
                    .collect(toImmutableList());

            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(MongoColumnHandle.class::cast)
                    .map(column -> new Descriptor.Field(column.getBaseName(), Optional.of(column.getType())))
                    .collect(toImmutableList()));

            CMDBQueryFunctionHandle handle = new CMDBQueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }
    }

    private static String getStringArgument(Map<String, Argument> arguments, String argName) {
        Slice slice = ((Slice) ((ScalarArgument) arguments.get(argName)).getValue());
        return slice != null ? slice.toStringUtf8() : "";
    }

    private static String regexFilter(String fields, String regex) {
        if (fields != null && regex != null) {
            return String.format("""
                        {%s: {$regex: "%s", $options: "i"}}
                        """, fields, regex);
        }
        return "{}";
    }

    public static Document parseFilter(String filter)
    {
        try {
            return Document.parse(filter);
        }
        catch (JsonParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Can't parse 'filter' argument as json", e);
        }
    }

    public static class CMDBQueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final MongoTableHandle tableHandle;

        @JsonCreator
        public CMDBQueryFunctionHandle(@JsonProperty("tableHandle") MongoTableHandle tableHandle)
        {
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        }

        @JsonProperty
        public ConnectorTableHandle getTableHandle()
        {
            return tableHandle;
        }
    }
}
