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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.*;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimestampType;

import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.influxdb.TypeUtils.isPushdownSupportedType;

import io.trino.plugin.influxdb.ptf.RawQuery.RawQueryFunctionHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnName.TIME;
import static io.trino.plugin.influxdb.InfluxConstant.ColumnKind;
import io.trino.spi.type.Type;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class InfluxMetadata
        implements ConnectorMetadata {
    private static final Pattern SELECT_LIST_PATTERN = Pattern.compile("(?is)\\bselect\\b(.*?)\\bfrom\\b");
    private static final Pattern GROUP_BY_PATTERN = Pattern.compile("(?is)\\bgroup\\s+by\\b(.*?)(?:\\border\\s+by\\b|\\blimit\\b|\\boffset\\b|\\bslimit\\b|\\bsoffset\\b|\\bfill\\b|\\btz\\b|;|$)");
    private static final Pattern FUNCTION_PREFIX_PATTERN = Pattern.compile("^([a-zA-Z_][a-zA-Z0-9_]*)\\s*\\(");
    private final InfluxClient client;

    @Inject
    public InfluxMetadata(InfluxClient client) {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(client.getSchemaNames()));

        return schemaNames.stream()
                .flatMap(schemaName -> client.getSchemaTableNames(schemaName).stream())
                .collect(toImmutableList());
    }

    @Override
    public InfluxTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName) {
        return client.getTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .map(table -> new InfluxTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName(), ImmutableList.of(), Optional.empty())).orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return getTableMetadata(((InfluxTableHandle) tableHandle).toSchemaTableName())
                .orElseThrow(() -> new RuntimeException("The table handle is invalid " + tableHandle));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        InfluxTableHandle influxTableHandle = (InfluxTableHandle) tableHandle;

        InfluxTableHandle table = client.getTableHandle(influxTableHandle.getSchemaName(), influxTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(influxTableHandle.toSchemaTableName()));

        ImmutableMap.Builder<String, ColumnHandle> columnHandles =
                ImmutableMap.builderWithExpectedSize(table.getColumns().size());
        for (InfluxColumnHandle column : table.getColumns()) {
            columnHandles.put(column.getName(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        InfluxColumnHandle influxColumnHandle = (InfluxColumnHandle) columnHandle;
        return new ColumnMetadata(influxColumnHandle.getName(), influxColumnHandle.getType());
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
            if (!prefix.matches(tableName)) {
                continue;
            }
            getTableMetadata(tableName).ifPresent(tableMetadata -> columns.put(tableName, tableMetadata.getColumns()));
        }
        return columns.buildOrThrow().entrySet().stream()
                .map(entry -> TableColumnsMetadata.forTable(entry.getKey(), entry.getValue())).iterator();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit) {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        // InfluxQL Limit 0 is equivalent to setting no limit
        if (limit == 0) {
            return Optional.empty();
        }
        // InfluxQL doesn't support limit number greater than integer max
        if (limit > Integer.MAX_VALUE) {
            return Optional.empty();
        }
        if (tableHandle.getLimit().isPresent() && tableHandle.getLimit().getAsInt() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                tableHandle.withLimit(OptionalInt.of(toIntExact(limit))),
                true,
                false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint) {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        if(tableHandle.getQuery().isPresent()){
            return Optional.empty();
        }
        TupleDomain<ColumnHandle> oldDomain = tableHandle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
        } else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();
            domains.forEach((key, domain) -> {
                if (isPushdownSupportedType(((InfluxColumnHandle) key).getType())
                        && isPushdownSupportedDomain(domain)) {
                    supported.put(key, domain);
                } else {
                    unsupported.put(key, domain);
                }
            });
            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                tableHandle.withConstraint(newDomain),
                remainingFilter,
                false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments) {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        List<ColumnHandle> oldProjections = ((InfluxTableHandle) handle).getProjections();
        List<ColumnHandle> newProjections = ImmutableList.copyOf(assignments.values());
        if (oldProjections.equals(newProjections)) {
            return Optional.empty();
        }

        List<Assignment> assignmentsList = assignments.entrySet().stream()
                .map(assignment -> new Assignment(
                        assignment.getKey(),
                        assignment.getValue(),
                        ((InfluxColumnHandle) assignment.getValue()).getType()))
                .collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                tableHandle.withProjections(newProjections),
                projections,
                assignmentsList,
                false));
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle) {
        if (!(handle instanceof RawQueryFunctionHandle)) {
            return Optional.empty();
        }

        ConnectorTableHandle tableHandle = ((RawQueryFunctionHandle) handle).getTableHandle();
        List<ColumnHandle> columnHandles = resolveTableFunctionColumns(session, (InfluxTableHandle) tableHandle);
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    public List<ColumnHandle> resolveTableFunctionColumns(ConnectorSession session, InfluxTableHandle tableHandle) {
        if (tableHandle.getQuery().isEmpty()) {
            return getTableFunctionColumns(session, tableHandle);
        }

        String query = tableHandle.getQuery().orElseThrow();
        String schema = tableHandle.getSchemaName();

        try {
            org.influxdb.dto.Query influxQuery = new org.influxdb.dto.Query(query, schema);
            InfluxRecord queryResult = client.query(influxQuery);

            if (queryResult.getColumns().isEmpty()) {
                List<ColumnHandle> inferredColumns = inferColumnsFromInfluxQl(session, tableHandle, query);
                if (!inferredColumns.isEmpty()) {
                    return inferredColumns;
                }
                return defaultEmptyQueryColumns();
            }

            return queryResult.getColumns().stream()
                    .map(columnName -> new InfluxColumnHandle(columnName, inferColumnType(columnName, queryResult), inferColumnKind(columnName)))
                    .map(ColumnHandle.class::cast)
                    .collect(toImmutableList());
        }
        catch (Exception e) {
            List<ColumnHandle> inferredColumns = inferColumnsFromInfluxQl(session, tableHandle, query);
            if (!inferredColumns.isEmpty()) {
                return inferredColumns;
            }
            return getTableFunctionColumns(session, tableHandle);
        }
    }

    private List<ColumnHandle> inferColumnsFromInfluxQl(ConnectorSession session, InfluxTableHandle tableHandle, String query)
    {
        Map<String, InfluxColumnHandle> baseColumnsByName = getBaseColumnsByName(session, tableHandle);
        List<SelectItem> selectItems = parseSelectItems(query);
        List<String> groupByItems = parseGroupByItems(query);

        if (selectItems.isEmpty() && groupByItems.isEmpty()) {
            return ImmutableList.of();
        }

        Map<String, ColumnHandle> resolvedColumns = new LinkedHashMap<>();
        addResolvedColumn(resolvedColumns, new InfluxColumnHandle(TIME.getName(), TIMESTAMP_NANOS, ColumnKind.TIME));

        for (SelectItem selectItem : selectItems) {
            InfluxColumnHandle baseColumn = baseColumnsByName.get(selectItem.normalizedExpression().toLowerCase(Locale.ENGLISH));
            InfluxColumnHandle column = buildSelectColumn(selectItem, baseColumn);
            if (!column.getName().equalsIgnoreCase(TIME.getName())) {
                addResolvedColumn(resolvedColumns, column);
            }
        }

        for (String groupByItem : groupByItems) {
            if (isTimeExpression(groupByItem)) {
                continue;
            }
            String normalizedGroupByItem = normalizeIdentifier(groupByItem);
            if (normalizedGroupByItem.isEmpty()) {
                continue;
            }

            InfluxColumnHandle baseColumn = baseColumnsByName.get(normalizedGroupByItem.toLowerCase(Locale.ENGLISH));
            if (baseColumn != null) {
                addResolvedColumn(resolvedColumns, baseColumn);
                continue;
            }

            addResolvedColumn(resolvedColumns, new InfluxColumnHandle(normalizedGroupByItem, VARCHAR, ColumnKind.TAG));
        }

        return ImmutableList.copyOf(resolvedColumns.values());
    }

    private List<ColumnHandle> getTableFunctionColumns(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ConnectorTableSchema tableSchema = getTableSchema(session, tableHandle);
        Map<String, ColumnHandle> columnHandlesByName = getColumnHandles(session, tableHandle);
        return tableSchema.getColumns().stream()
                .map(ColumnSchema::getName)
                .map(columnHandlesByName::get)
                .collect(toImmutableList());
    }

    private List<ColumnHandle> defaultEmptyQueryColumns() {
        return ImmutableList.of(new InfluxColumnHandle(TIME.getName(), TIMESTAMP_NANOS, ColumnKind.TIME));
    }

    private Map<String, InfluxColumnHandle> getBaseColumnsByName(ConnectorSession session, InfluxTableHandle tableHandle)
    {
        try {
            return getTableFunctionColumns(session, tableHandle).stream()
                    .map(InfluxColumnHandle.class::cast)
                    .collect(ImmutableMap.toImmutableMap(
                            column -> column.getName().toLowerCase(Locale.ENGLISH),
                            column -> column,
                            (left, right) -> left));
        }
        catch (RuntimeException ignored) {
            return ImmutableMap.of();
        }
    }

    private List<SelectItem> parseSelectItems(String query)
    {
        Matcher matcher = SELECT_LIST_PATTERN.matcher(query);
        if (!matcher.find()) {
            return ImmutableList.of();
        }

        return splitTopLevelComma(matcher.group(1)).stream()
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .map(this::parseSelectItem)
                .collect(toImmutableList());
    }

    private SelectItem parseSelectItem(String item)
    {
        int aliasStart = findTopLevelAlias(item);
        if (aliasStart >= 0) {
            AliasParts aliasParts = splitAlias(item, aliasStart);
            String expression = aliasParts.expression();
            String alias = normalizeIdentifier(aliasParts.alias());
            return new SelectItem(expression, alias.isEmpty() ? normalizeIdentifier(expression) : alias);
        }
        return new SelectItem(item, normalizeIdentifier(item));
    }

    private List<String> parseGroupByItems(String query)
    {
        Matcher matcher = GROUP_BY_PATTERN.matcher(query);
        if (!matcher.find()) {
            return ImmutableList.of();
        }

        return splitTopLevelComma(matcher.group(1)).stream()
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .collect(toImmutableList());
    }

    private List<String> splitTopLevelComma(String input)
    {
        ImmutableList.Builder<String> parts = ImmutableList.builder();
        StringBuilder current = new StringBuilder();
        int parenthesesDepth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < input.length(); i++) {
            char currentChar = input.charAt(i);
            if (currentChar == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            }
            else if (currentChar == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            }
            else if (!inSingleQuote && !inDoubleQuote) {
                if (currentChar == '(') {
                    parenthesesDepth++;
                }
                else if (currentChar == ')') {
                    parenthesesDepth = Math.max(0, parenthesesDepth - 1);
                }
                else if (currentChar == ',' && parenthesesDepth == 0) {
                    parts.add(current.toString());
                    current = new StringBuilder();
                    continue;
                }
            }
            current.append(currentChar);
        }

        parts.add(current.toString());
        return parts.build();
    }

    private int findTopLevelAlias(String item)
    {
        int parenthesesDepth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < item.length(); i++) {
            char currentChar = item.charAt(i);
            if (currentChar == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            }
            else if (currentChar == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            }
            else if (!inSingleQuote && !inDoubleQuote) {
                if (currentChar == '(') {
                    parenthesesDepth++;
                }
                else if (currentChar == ')') {
                    parenthesesDepth = Math.max(0, parenthesesDepth - 1);
                }
                else if (parenthesesDepth == 0 && startsWithAsToken(item, i)) {
                    return i;
                }
            }
        }
        return -1;
    }

    private InfluxColumnHandle buildSelectColumn(SelectItem selectItem, InfluxColumnHandle baseColumn)
    {
        String normalizedExpression = selectItem.normalizedExpression();
        if (selectItem.columnName().equalsIgnoreCase(TIME.getName()) || isTimeExpression(normalizedExpression)) {
            return new InfluxColumnHandle(TIME.getName(), TIMESTAMP_NANOS, ColumnKind.TIME);
        }

        if (baseColumn != null && selectItem.columnName().equalsIgnoreCase(normalizedExpression)) {
            return baseColumn;
        }

        return new InfluxColumnHandle(
                selectItem.columnName(),
                inferQueryColumnType(selectItem.expression(), baseColumn),
                inferQueryColumnKind(selectItem.expression(), baseColumn));
    }

    private Type inferQueryColumnType(String expression, InfluxColumnHandle baseColumn)
    {
        if (baseColumn != null) {
            return baseColumn.getType();
        }

        String normalizedExpression = normalizeIdentifier(expression).toLowerCase(Locale.ENGLISH);
        if (extractFunctionName(normalizedExpression).filter("count"::equals).isPresent()) {
            return BIGINT;
        }
        if (isTimeExpression(expression)) {
            return TIMESTAMP_NANOS;
        }
        return DOUBLE;
    }

    private ColumnKind inferQueryColumnKind(String expression, InfluxColumnHandle baseColumn)
    {
        if (baseColumn != null) {
            return baseColumn.getKind();
        }
        if (isTimeExpression(expression)) {
            return ColumnKind.TIME;
        }
        return ColumnKind.FIELD;
    }

    private boolean isTimeExpression(String expression)
    {
        String normalizedExpression = normalizeIdentifier(expression).toLowerCase(Locale.ENGLISH);
        return normalizedExpression.equals(TIME.getName()) || extractFunctionName(normalizedExpression).filter(TIME.getName()::equals).isPresent();
    }

    private static String normalizeIdentifier(String identifier)
    {
        String normalized = identifier.trim();
        if (normalized.endsWith(";")) {
            normalized = normalized.substring(0, normalized.length() - 1).trim();
        }
        if (normalized.startsWith("\"") && normalized.endsWith("\"") && normalized.length() >= 2) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return normalized;
    }

    private void addResolvedColumn(Map<String, ColumnHandle> resolvedColumns, InfluxColumnHandle column)
    {
        resolvedColumns.putIfAbsent(column.getName().toLowerCase(Locale.ENGLISH), column);
    }

    private static boolean startsWithAsToken(String value, int start)
    {
        if (start < 0 || start >= value.length()) {
            return false;
        }
        if (value.charAt(start) != 'a' && value.charAt(start) != 'A') {
            return false;
        }

        int nextToken = skipWhitespace(value, start + 1);
        if (nextToken >= value.length() || (value.charAt(nextToken) != 's' && value.charAt(nextToken) != 'S')) {
            return false;
        }

        int aliasStart = skipWhitespace(value, nextToken + 1);
        return aliasStart < value.length();
    }

    private static AliasParts splitAlias(String value, int aliasTokenStart)
    {
        int aIndex = skipWhitespace(value, aliasTokenStart);
        int sIndex = skipWhitespace(value, aIndex + 1);
        int aliasStart = skipWhitespace(value, sIndex + 1);
        return new AliasParts(value.substring(0, aliasTokenStart).trim(), value.substring(aliasStart));
    }

    private static int skipWhitespace(String value, int start)
    {
        int index = start;
        while (index < value.length() && Character.isWhitespace(value.charAt(index))) {
            index++;
        }
        return index;
    }

    private static Optional<String> extractFunctionName(String expression)
    {
        Matcher matcher = FUNCTION_PREFIX_PATTERN.matcher(expression);
        if (matcher.find()) {
            return Optional.of(matcher.group(1).toLowerCase(Locale.ENGLISH));
        }
        return Optional.empty();
    }

    private record SelectItem(String expression, String columnName)
    {
        private String normalizedExpression()
        {
            return normalizeIdentifier(expression);
        }
    }

    private record AliasParts(String expression, String alias) {}

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName schemaTableName) {
        Optional<InfluxTableHandle> tableHandle = client.getTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName());

        return tableHandle.map(table -> {
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder =
                    ImmutableList.builderWithExpectedSize(table.getColumns().size());
            List<InfluxColumnHandle> columns = table.getColumns();
            for (InfluxColumnHandle column : columns) {
                columnMetadataBuilder.add(new ColumnMetadata(column.getName(), column.getType()));
            }
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        });
    }

    private boolean isPushdownSupportedDomain(Domain domain) {
        if (domain.getValues() instanceof SortedRangeSet rangeSet) {
            if (rangeSet.getOrderedRanges().isEmpty()) {
                return false;
            }
            List<Range> ranges = rangeSet.getOrderedRanges();
            return isRangeSupportsTimestamp(ranges) ||
                    isRangeSupportsBoolean(ranges) ||
                    isRangeSupportsNumber(ranges) ||
                    isRangeSupportsVarchar(ranges);
        } else if (domain.getValues() instanceof EquatableValueSet valueSet) {
            return !valueSet.getDiscreteSet().isEmpty();
        }
        return false;
    }

    private static boolean isRangeSupportsVarchar(List<Range> ranges) {
        return ranges.stream().allMatch(range -> range.getType() == VARCHAR) &&
                ranges.stream().anyMatch(Range::isSingleValue);
    }

    private static boolean isRangeSupportsBoolean(List<Range> ranges) {
        return ranges.stream().allMatch(range -> range.getType() == BOOLEAN) &&
                ranges.stream().anyMatch(Range::isSingleValue);
    }

    private static boolean isRangeSupportsNumber(List<Range> ranges) {
        return ranges.stream().allMatch(range -> range.getType() == DOUBLE || range.getType() == BIGINT);
    }

    private static boolean isRangeSupportsTimestamp(List<Range> ranges) {
        return ranges.stream().allMatch(range -> range.getType() instanceof TimestampType);
    }

    /**
     * Infer the column type based on the column name and query result data.
     * This is used for dynamic column schema resolution in raw_query functions.
     */
    private Type inferColumnType(String columnName, InfluxRecord queryResult) {
        // Time column is always timestamp
        if (columnName.equals(TIME.getName())) {
            return TIMESTAMP_NANOS;
        }

        // Try to infer type from the actual data values
        if (!queryResult.getValues().isEmpty()) {
            List<Object> firstRow = queryResult.getValues().get(0);
            int columnIndex = queryResult.getColumns().indexOf(columnName);

            if (columnIndex >= 0 && columnIndex < firstRow.size()) {
                Object value = firstRow.get(columnIndex);
                if (value instanceof Number) {
                    return DOUBLE; // Default numeric type for InfluxDB
                }
                if (value instanceof Boolean) {
                    return BOOLEAN;
                }
                if (value instanceof String) {
                    return VARCHAR;
                }
            }
        }

        // Default fallback - most InfluxDB values are numeric
        return DOUBLE;
    }

    /**
     * Infer the column kind based on the column name.
     * This is used for dynamic column schema resolution in raw_query functions.
     */
    private ColumnKind inferColumnKind(String columnName) {
        // Time column is always TIME kind
        if (columnName.equals(TIME.getName())) {
            return ColumnKind.TIME;
        }

        // For raw query results, most columns are computed fields (aggregations, etc.)
        // so we default to FIELD kind
        return ColumnKind.FIELD;
    }

    public InfluxClient getClient() {
        return client;
    }
}
