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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DatainsightRecordSet
        implements RecordSet {
    private final DatainsightRecord sourceData;
    private final List<DatainsightColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    public DatainsightRecordSet(DatainsightTableHandle tableHandle, List<DatainsightColumnHandle> columnHandles, DatainsightClient client) {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(columnHandles, "columnHandles is null");


        this.sourceData = querySourceData(client, tableHandle, columnHandles);
        // column按sourceData的列顺序，获取列类型

        List<Map<String,String>> columns = sourceData.getColumns();
        List<DatainsightColumnHandle> sortedColumnHandles = Lists.newArrayListWithExpectedSize(columns.size());
        for (Map<String, String> column : columns) {
            String name = column.get("name");
            for (DatainsightColumnHandle columnHandle : columnHandles) {
                if (columnHandle.getName().equals(name)) {
                    sortedColumnHandles.add(columnHandle);
                    break;
                }
            }
        }
        this.columnHandles = ImmutableList.copyOf(sortedColumnHandles);
        this.columnTypes = this.columnHandles.stream()
                .map(DatainsightColumnHandle::getType)
                .collect(toImmutableList());


    }

    private DatainsightRecord querySourceData(DatainsightClient client, DatainsightTableHandle tableHandle, List<DatainsightColumnHandle> columnHandles) {
        Optional<String> query = tableHandle.getQuery();
        if (tableHandle.getType() == DatainsightTableHandle.Type.RAW) {
            return client.executeQuery(tableHandle.getIndex(), query.get());
        } else if (tableHandle.getType() == DatainsightTableHandle.Type.PPL ||
                tableHandle.getType() == DatainsightTableHandle.Type.SQL) {
            return client.getPluginResult(tableHandle.getType().toString(), query.get());
        } else {
            throw new IllegalArgumentException("Unknown table type: " + tableHandle.getType());
        }
    }

    @VisibleForTesting
    public DatainsightRecord getSourceData() {
        return sourceData;
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new DatainsightRecordCursor(columnHandles, sourceData);
    }
}
