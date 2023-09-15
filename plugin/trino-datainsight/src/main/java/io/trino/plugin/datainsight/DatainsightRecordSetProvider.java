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

import com.google.inject.Inject;
import io.trino.spi.connector.*;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DatainsightRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final DatainsightClient client;

    @Inject
    public DatainsightRecordSetProvider(DatainsightClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        DatainsightTableHandle tableHandle = (DatainsightTableHandle) table;
        List<DatainsightColumnHandle> columnHandles = columns.stream()
                .map(DatainsightColumnHandle.class::cast)
                .collect(toImmutableList());

        return new DatainsightRecordSet(tableHandle, columnHandles, client);
    }
}
