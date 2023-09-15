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
package io.trino.plugin.mysql.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.plugin.jdbc.*;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.*;
import io.trino.spi.function.table.Descriptor.Field;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class QueryCustom
        implements Provider<ConnectorTableFunction>
{
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "custom";

    private final JdbcTransactionManager transactionManager;

    @Inject
    public QueryCustom(JdbcTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorTableFunction get()
    {
        return new ClassLoaderSafeConnectorTableFunction(new QueryFunction(transactionManager), getClass().getClassLoader());
    }

    public static class QueryFunction
            extends AbstractConnectorTableFunction
    {
        private final JdbcTransactionManager transactionManager;

        public QueryFunction(JdbcTransactionManager transactionManager)
        {
            super(
                    SCHEMA_NAME,
                    NAME,
                    ImmutableList.of(
                            ScalarArgumentSpecification.builder()
                                .name("QUERY")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                            ScalarArgumentSpecification.builder()
                                .name("SCENE")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                            ScalarArgumentSpecification.builder()
                                .name("SERVICENAME")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                            ScalarArgumentSpecification.builder()
                                .name("STARTTIME")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                            ScalarArgumentSpecification.builder()
                                .name("ENDTIME")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build(),
                            ScalarArgumentSpecification.builder()
                                .name("TICKETSTATUS")
                                .type(VARCHAR)
                                .defaultValue(null)
                                .build()),
                    GENERIC_TABLE);
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl)
        {
            String query = getStringArgument(arguments, "QUERY");
            String scene = getStringArgument(arguments, "SCENE");
            String ticket_status = getStringArgument(arguments, "TICKETSTATUS");
            String service_name = getStringArgument(arguments, "SERVICENAME");
            String start_time = getStringArgument(arguments, "STARTTIME");
            String end_time = getStringArgument(arguments, "ENDTIME");


            switch (scene) {
                // 主机agent状态
                case "agent_status" -> query = """
                        SELECT
                           *
                        FROM
                            (
                            SELECT
                                c.bk_host_id,
                                c.STATUS,
                                d.bk_biz_id,
                                d.bk_host_name
                            FROM
                                bk_nodeman.node_man_processstatus AS c
                                JOIN bk_nodeman.node_man_host AS d ON c.bk_host_id = d.bk_host_id
                            WHERE
                            c.proc_type = 'AGENT'
                            ) AS e
                        """;

                // 工单筛选
                case "ticket_filter" -> {
                    query = """
                            SELECT
                                ticket.create_at,
                                ticket.sn,
                                ticket.title,
                                ticket.current_status,
                                ticket.updated_by,
                                service.NAME AS service_name,
                                (
                                    SELECT servicecategory.name
                                    FROM bk_itsm.service_servicecategory AS servicecategory
                                    WHERE servicecategory.KEY = service.KEY
                                    LIMIT 1
                                ) AS service_category_name,
                                state_status_max.NAME AS state_name,
                                GROUP_CONCAT(
                                    CASE WHEN processors.role_type = 'group' THEN role_userrole.NAME ELSE processors.username END
                                    SEPARATOR ', '
                                ) AS usernames
                            FROM
                                bk_itsm.ticket_ticket AS ticket
                                JOIN bk_itsm.service_service AS service ON ticket.service_id = service.id
                                LEFT JOIN bk_itsm.ticket__ticketuser_current_processors AS processors ON ticket.id = processors.ticket_id
                                JOIN (
                                    SELECT
                                        t1.ticket_id,
                                        t1.max_state_id,
                                        t2.NAME
                                    FROM
                                        (
                                            SELECT ticket_id, MAX( state_id ) AS max_state_id
                                            FROM bk_itsm.ticket_status
                                            GROUP BY ticket_id
                                        ) AS t1
                                        JOIN bk_itsm.ticket_status AS t2 ON t1.ticket_id = t2.ticket_id
                                        AND t1.max_state_id = t2.state_id
                                ) AS state_status_max ON ticket.id = state_status_max.ticket_id
                                LEFT JOIN bk_itsm.role_userrole AS role_userrole ON processors.username = role_userrole.id
                            WHERE
                                1 = 1
                                """;

                    if (!service_name.isEmpty()) {
                        query += String.format("AND service.name LIKE '%%%s%%'", service_name);
                    }

                    if (!ticket_status.isEmpty()) {
                        // 将逗号分隔的字符串分割成字符串数组
                        String[] statusArray = ticket_status.split(",");
                        // 构建 IN 子句
                        StringBuilder ticketInClause = new StringBuilder();
                        for (int i = 0; i < statusArray.length; i++) {
                            ticketInClause.append("'");
                            ticketInClause.append(statusArray[i].trim()); // 移除首尾空格
                            ticketInClause.append("'");
                            if (i < statusArray.length - 1) {
                                ticketInClause.append(", "); // 添加逗号分隔符
                            }
                        }

                        query += String.format("AND current_status IN (%s)", ticketInClause);
                    }

                    if (!start_time.isEmpty()) {
                        query += String.format("AND ticket.create_at >= DATE_FORMAT('%s', '%%Y-%%m-%%d %%H:%%i:%%s')", start_time);
                    }

                    if (!end_time.isEmpty()) {
                        query += String.format("AND ticket.create_at <= DATE_FORMAT('%s', '%%Y-%%m-%%d %%H:%%i:%%s')", end_time);
                    }

                    query += """
                        GROUP BY
                            ticket.create_at,
                            ticket.sn,
                            ticket.title,
                            ticket.updated_by,
                            service.NAME,
                            service.KEY,
                            state_status_max.NAME,
                            ticket.current_status
                            """;
                }
            }

            PreparedQuery preparedQuery = new PreparedQuery(query, ImmutableList.of());

            JdbcMetadata metadata = transactionManager.getMetadata(transaction);
            JdbcTableHandle tableHandle = metadata.getTableHandle(session, preparedQuery);
            List<JdbcColumnHandle> columns = tableHandle.getColumns().orElseThrow(() -> new IllegalStateException("Handle doesn't have columns info"));
            Descriptor returnedType = new Descriptor(columns.stream()
                    .map(column -> new Field(column.getColumnName(), Optional.of(column.getColumnType())))
                    .collect(toImmutableList()));

            ITSMQueryFunctionHandle handle = new ITSMQueryFunctionHandle(tableHandle);

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

    public static class ITSMQueryFunctionHandle
            implements ConnectorTableFunctionHandle
    {
        private final JdbcTableHandle tableHandle;

        @JsonCreator
        public ITSMQueryFunctionHandle(@JsonProperty("tableHandle") JdbcTableHandle tableHandle)
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
