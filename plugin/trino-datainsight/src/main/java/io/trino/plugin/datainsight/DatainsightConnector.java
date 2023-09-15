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
package io.trino.plugin.datainsight;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Set;

import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class DatainsightConnector
        implements Connector {
    private final LifeCycleManager lifeCycleManager;
    private final DatainsightMetadata metadata;
    private final DatainsightSplitManager splitManager;
    private final DatainsightRecordSetProvider recordSetProviderProvider;
    private final NodesSystemTable nodesSystemTable;
    private final Set<ConnectorTableFunction> connectorTableFunctions;

    @Inject
    public DatainsightConnector(
            LifeCycleManager lifeCycleManager,
            DatainsightMetadata metadata,
            DatainsightSplitManager splitManager,
            DatainsightRecordSetProvider recordSetProviderProvider,
            NodesSystemTable nodesSystemTable,
            Set<ConnectorTableFunction> connectorTableFunctions

    ) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProviderProvider = requireNonNull(recordSetProviderProvider, "pageSourceProvider is null");
        this.nodesSystemTable = requireNonNull(nodesSystemTable, "nodesSystemTable is null");
        this.connectorTableFunctions = ImmutableSet.copyOf(requireNonNull(connectorTableFunctions, "connectorTableFunctions is null"));
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return DatainsightTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProviderProvider;
    }

    @Override
    public Set<SystemTable> getSystemTables() {
        return ImmutableSet.of(nodesSystemTable);
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return connectorTableFunctions;
    }

    @Override
    public final void shutdown() {
        lifeCycleManager.stop();
    }
}
