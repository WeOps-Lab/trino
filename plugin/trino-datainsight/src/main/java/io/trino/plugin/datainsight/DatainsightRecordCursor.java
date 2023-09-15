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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class DatainsightRecordCursor
        implements RecordCursor
{
    private long totalBytes;
    private final List<DatainsightColumnHandle> columnHandles;
    private final Iterator<List<Object>> sourceDataIterator;
    private List<Object> fieldValues;

    public DatainsightRecordCursor(List<DatainsightColumnHandle> columnHandles, DatainsightRecord sourceData)
    {
        List<Map<String,String>> columns;
        List<List<Object>> recordSet;

        //even column not contains 'time', it still returns as the first field in Datainsight-client query result

        columns = sourceData.getColumns();
        recordSet = sourceData.getValues();

        this.columnHandles = ImmutableList.copyOf(columnHandles);
        List<List<Object>> alignedRecordSet = Lists.newArrayListWithExpectedSize(recordSet.size());
        Map<String, Integer> nameIdxMap = IntStream.range(0, columns.size())
                .boxed()
                .collect(toMap(idx -> columns.get(idx).get("name"), identity()));
//        List<String> names = columnHandles.stream().map(DatainsightColumnHandle::getName).toList();
        List<String> names = columns.stream().map(column -> column.get("name")).toList();
        for (List<Object> record : recordSet) {
            List<Object> alignedRecord = Lists.newArrayListWithExpectedSize(names.size());
            for (String name : names) {
                Object value = record.get(nameIdxMap.get(name));
                alignedRecord.add(value);
            }
            alignedRecordSet.add(alignedRecord);
        }

        this.sourceDataIterator = alignedRecordSet.iterator();
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!sourceDataIterator.hasNext()) {
            return false;
        }
        fieldValues = sourceDataIterator.next();
        totalBytes += fieldValues.size();
        return true;
    }

    private Object getFieldValue(int field)
    {
        checkState(fieldValues != null, "Cursor has not been advanced yet");
        return fieldValues.get(field);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        Object value = getFieldValue(field);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        return false;
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        Object value = getFieldValue(field);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        Object value = getFieldValue(field);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0;
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        Object value = getFieldValue(field);
        return utf8Slice(value.toString());
    }

    @Override
    public Object getObject(int field)
    {
        Type type = getType(field);
        Object value = getFieldValue(field);
        if (type == TIMESTAMP_NANOS) {
            Instant utc = LocalDateTime.parse((String) value, ISO_DATE_TIME).atZone(ZoneId.of("UTC")).toInstant();
            return TimestampUtils.longTimestamp(utc);
        }
        else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Objects.isNull(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(expected.equals(actual), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
