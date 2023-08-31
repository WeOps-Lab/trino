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
package io.trino.plugin.mysql;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.ptf.Procedure.ProcedureFunctionHandle;
import io.trino.plugin.jdbc.ptf.Query.QueryFunctionHandle;
import io.trino.plugin.mysql.ptf.QueryITSM.ITSMQueryFunctionHandle;

import io.trino.spi.connector.*;

import io.trino.spi.function.table.ConnectorTableFunctionHandle;


import java.util.*;

import static java.util.Objects.requireNonNull;


public class MysqlMetadata
        extends DefaultJdbcMetadata
{

    public final JdbcClient mysqlClient;
    @Inject
    public MysqlMetadata(JdbcClient mysqlClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners) {
        super(mysqlClient, false, jdbcQueryEventListeners);
        this.mysqlClient = requireNonNull(mysqlClient, "mysqlClient is null");
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof QueryFunctionHandle queryFunctionHandle) {
            return Optional.of(getTableFunctionApplicationResult(session, queryFunctionHandle.getTableHandle()));
        }
        if (handle instanceof ProcedureFunctionHandle procedureFunctionHandle) {
            return Optional.of(getTableFunctionApplicationResult(session, procedureFunctionHandle.getTableHandle()));
        }
        if (handle instanceof ITSMQueryFunctionHandle procedureFunctionHandle) {
            return Optional.of(getTableFunctionApplicationResult(session, procedureFunctionHandle.getTableHandle()));
        }
        return Optional.empty();
    }

    private TableFunctionApplicationResult<ConnectorTableHandle> getTableFunctionApplicationResult(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        List<ColumnHandle> columnHandles = ImmutableList.copyOf(getColumnHandles(session, tableHandle).values());
        return new TableFunctionApplicationResult<>(tableHandle, columnHandles);
    }
}

