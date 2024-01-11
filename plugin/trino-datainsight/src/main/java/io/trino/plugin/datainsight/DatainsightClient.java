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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ForwardingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import io.airlift.json.JsonCodec;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.ILoggerFactory;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.datainsight.DatainsightErrorCode.*;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class DatainsightClient {
    private static final Logger LOG = Logger.get(DatainsightClient.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");

    private final RestClient client;

    private final MongoClient mongoClient;
    private final String mongoDb;
    private final int scrollSize;
    private final Duration scrollTimeout;
    private final int cacheExpireTime;

    private final Cache cache = new Cache();

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("NodeRefresher"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final Duration refreshInterval;
    private final boolean tlsEnabled;
    private final boolean ignorePublishAddress;

    private final TimeStat searchStats = new TimeStat(MILLISECONDS);
    private final TimeStat nextPageStats = new TimeStat(MILLISECONDS);
    private final TimeStat countStats = new TimeStat(MILLISECONDS);
    private final TimeStat backpressureStats = new TimeStat(MILLISECONDS);

    private static final Map<String, Class<?>> STRING_CLASS_MAP = new HashMap<>();

    static {
        STRING_CLASS_MAP.put("text", String.class);
        STRING_CLASS_MAP.put("keyword", String.class);
        STRING_CLASS_MAP.put("byte", Byte.class);
        STRING_CLASS_MAP.put("long", Long.class);
        STRING_CLASS_MAP.put("integer", Integer.class);
        STRING_CLASS_MAP.put("timestamp", String.class);
        STRING_CLASS_MAP.put("boolean", Boolean.class);
        STRING_CLASS_MAP.put("float", Float.class);
        STRING_CLASS_MAP.put("double", Double.class);
        STRING_CLASS_MAP.put("string", String.class);
        STRING_CLASS_MAP.put("ip", String.class);
        STRING_CLASS_MAP.put("binary", String.class);
        STRING_CLASS_MAP.put("struct", Object.class);
        STRING_CLASS_MAP.put("array", Object.class);
    }

    @Inject
    public DatainsightClient(
            DatainsightConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig) {

        client = createOpenserachClient(config);
        mongoClient = createMongoClient(config);
        mongoDb = config.getMongoDb();
        this.ignorePublishAddress = config.isIgnorePublishAddress();
        this.scrollSize = config.getScrollSize();
        this.scrollTimeout = config.getScrollTimeout();
        this.refreshInterval = config.getNodeRefreshInterval();
        this.tlsEnabled = config.isTlsEnabled();
        this.cacheExpireTime = config.getCacheExpireTime();
    }


    @PreDestroy
    public void close()
            throws IOException {
        client.close();
    }


    public static Class<?> getPluginType(String type) {
        return STRING_CLASS_MAP.getOrDefault(type, Object.class);
    }

    private static RestClient createOpenserachClient(
            DatainsightConfig config) {

        RestClient client = new RestClient(config);
        return client;

    }

    private static MongoClient createMongoClient(
            DatainsightConfig config) {
        String mongoUri = config.getMongoUri();
        MongoClient client = MongoClients.create(new ConnectionString(mongoUri));
        return client;

    }
//    private static AWSCredentialsProvider getAwsCredentialsProvider(AwsSecurityConfig config) {
//        AWSCredentialsProvider credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance();
//
//        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
//            credentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(
//                    config.getAccessKey().get(),
//                    config.getSecretKey().get()));
//        }
//
//        if (config.getIamRole().isPresent()) {
//            STSAssumeRoleSessionCredentialsProvider.Builder credentialsProviderBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(config.getIamRole().get(), "trino-session")
//                    .withStsClient(AWSSecurityTokenServiceClientBuilder.standard()
//                            .withRegion(config.getRegion())
//                            .withCredentials(credentialsProvider)
//                            .build());
//            config.getExternalId().ifPresent(credentialsProviderBuilder::withExternalId);
//            credentialsProvider = credentialsProviderBuilder.build();
//        }
//
//        return credentialsProvider;
//    }

//    private static Optional<SSLContext> buildSslContext(
//            Optional<File> keyStorePath,
//            Optional<String> keyStorePassword,
//            Optional<File> trustStorePath,
//            Optional<String> trustStorePassword) {
//        if (keyStorePath.isEmpty() && trustStorePath.isEmpty()) {
//            return Optional.empty();
//        }
//
//        try {
//            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
//        } catch (GeneralSecurityException | IOException e) {
//            throw new TrinoException(DATAINSIGHT_SSL_INITIALIZATION_FAILURE, e);
//        }
//    }

//    private Set<DatainsightNode> fetchNodes() {
//        NodesResponse nodesResponse = doRequest("/_nodes/http", NODES_RESPONSE_CODEC::fromJson);
//
//        ImmutableSet.Builder<DatainsightNode> result = ImmutableSet.builder();
//        for (Map.Entry<String, NodesResponse.Node> entry : nodesResponse.getNodes().entrySet()) {
//            String nodeId = entry.getKey();
//            NodesResponse.Node node = entry.getValue();
//
//            if (!Sets.intersection(node.getRoles(), NODE_ROLES).isEmpty()) {
//                Optional<String> address = node.getAddress()
//                        .flatMap(DatainsightClient::extractAddress);
//
//                result.add(new DatainsightNode(nodeId, address));
//            }
//        }
//
//        return result.build();
//    }

//    public Set<DatainsightNode> getNodes() {
//        return nodes.get();
//    }
//
//    public List<Shard> getSearchShards(String index) {
//        Map<String, DatainsightNode> nodeById = getNodes().stream()
//                .collect(toImmutableMap(DatainsightNode::getId, Function.identity()));
//
//        SearchShardsResponse shardsResponse = doRequest("GET", format("/%s/_search_shards", index), null, null);
//
//        ImmutableList.Builder<Shard> shards = ImmutableList.builder();
//        List<DatainsightNode> nodes = ImmutableList.copyOf(nodeById.values());
//
//        for (List<SearchShardsResponse.Shard> shardGroup : shardsResponse.getShardGroups()) {
//            Optional<SearchShardsResponse.Shard> candidate = shardGroup.stream()
//                    .filter(shard -> shard.getNode() != null && nodeById.containsKey(shard.getNode()))
//                    .min(this::shardPreference);
//
//            SearchShardsResponse.Shard chosen;
//            DatainsightNode node;
//            if (candidate.isEmpty()) {
//                // pick an arbitrary shard with and assign to an arbitrary node
//                chosen = shardGroup.stream()
//                        .min(this::shardPreference)
//                        .get();
//                node = nodes.get(chosen.getShard() % nodes.size());
//            } else {
//                chosen = candidate.get();
//                node = nodeById.get(chosen.getNode());
//            }
//
//            shards.add(new Shard(chosen.getIndex(), chosen.getShard(), node.getAddress()));
//        }
//
//        return shards.build();
//    }
//
//    private int shardPreference(SearchShardsResponse.Shard left, SearchShardsResponse.Shard right) {
//        // Favor non-primary shards
//        if (left.isPrimary() == right.isPrimary()) {
//            return 0;
//        }
//
//        return left.isPrimary() ? 1 : -1;
//    }
//
//    public boolean indexExists(String index) {
//        String path = format("/%s/_mappings", index);
//
//        try {
//            String response = client.performRequest("GET", path, null);
//            return true;
//        } catch (ResponseException e) {
//            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
//                return false;
//            }
//            throw new TrinoException(DATAINSIGHT_CONNECTION_ERROR, e);
//        } catch (IOException e) {
//            throw new TrinoException(DATAINSIGHT_CONNECTION_ERROR, e);
//        }
//    }

    public List<String> getIndexes() {

        return doRequest("GET", "/_cat/indices?h=index,docs.count,docs.deleted&format=json&s=index:asc", null, body -> {
            try {
                ImmutableList.Builder<String> result = ImmutableList.builder();
                JsonNode root = OBJECT_MAPPER.readTree(body);
                for (int i = 0; i < root.size(); i++) {
                    String index = root.get(i).get("index").asText();
                    // make sure the index has mappings we can use to derive the schema
                    int docsCount = root.get(i).get("docs.count").asInt();
                    int deletedDocsCount = root.get(i).get("docs.deleted").asInt();
                    if (docsCount == 0 && deletedDocsCount == 0) {
                        // without documents, the index won't have any dynamic mappings, but maybe there are some explicit ones
                        if (getIndexMetadata(index).getSchema().getFields().isEmpty()) {
                            continue;
                        }
                    }
                    result.add(index);
                }
                return result.build();
            } catch (IOException e) {
                throw new TrinoException(DATAINSIGHT_INVALID_RESPONSE, e);
            }
        });
    }

    public Map<String, List<String>> getAliases() {
        return doRequest("GET", "/_aliases", null, body -> {
            try {
                ImmutableMap.Builder<String, List<String>> result = ImmutableMap.builder();
                JsonNode root = OBJECT_MAPPER.readTree(body);

                Iterator<Map.Entry<String, JsonNode>> elements = root.fields();
                while (elements.hasNext()) {
                    Map.Entry<String, JsonNode> element = elements.next();
                    JsonNode aliases = element.getValue().get("aliases");
                    Iterator<String> aliasNames = aliases.fieldNames();
                    if (aliasNames.hasNext()) {
                        result.put(element.getKey(), ImmutableList.copyOf(aliasNames));
                    }
                }
                return result.buildOrThrow();
            } catch (IOException e) {
                throw new TrinoException(DATAINSIGHT_INVALID_RESPONSE, e);
            }
        });
    }

    public IndexMetadata getIndexMetadata(String index) {
        return new IndexMetadata(new IndexMetadata.ObjectType(new ArrayList<>()));
//        String path = format("/%s/_mappings", index);
//
//        return doRequest("GET", path, null, body -> {
//            try {
//                JsonNode mappings = OBJECT_MAPPER.readTree(body)
//                        .elements().next()
//                        .get("mappings");
//
//                if (!mappings.elements().hasNext()) {
//                    return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
//                }
//                if (!mappings.has("properties")) {
//                    // Older versions of datainsight supported multiple "type" mappings
//                    // for a given index. Newer versions support only one and don't
//                    // expose it in the document. Here we skip it if it's present.
//                    mappings = mappings.elements().next();
//
//                    if (!mappings.has("properties")) {
//                        return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
//                    }
//                }
//
//                JsonNode metaNode = nullSafeNode(mappings, "_meta");
//
//                JsonNode metaProperties = nullSafeNode(metaNode, "trino");
//
//                //stay backwards compatible with _meta.presto namespace for meta properties for some releases
//                if (metaProperties.isNull()) {
//                    metaProperties = nullSafeNode(metaNode, "presto");
//                }
//
//                return new IndexMetadata(parseType(mappings.get("properties"), metaProperties));
//            } catch (IOException e) {
//                throw new TrinoException(DATAINSIGHT_INVALID_RESPONSE, e);
//            }
//        });
    }

    private IndexMetadata.ObjectType parseType(JsonNode properties, JsonNode metaProperties) {
        Iterator<Map.Entry<String, JsonNode>> entries = properties.fields();

        ImmutableList.Builder<IndexMetadata.Field> result = ImmutableList.builder();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> field = entries.next();

            String name = field.getKey();
            JsonNode value = field.getValue();

            //default type is object
            String type = "object";
            if (value.has("type")) {
                type = value.get("type").asText();
            }
            JsonNode metaNode = nullSafeNode(metaProperties, name);
            boolean isArray = !metaNode.isNull() && metaNode.has("isArray") && metaNode.get("isArray").asBoolean();
            boolean asRawJson = !metaNode.isNull() && metaNode.has("asRawJson") && metaNode.get("asRawJson").asBoolean();

            // While it is possible to handle isArray and asRawJson in the same column by creating a ARRAY(VARCHAR) type, we chose not to take
            // this route, as it will likely lead to confusion in dealing with array syntax in Trino and potentially nested array and other
            // syntax when parsing the raw json.
            if (isArray && asRawJson) {
                throw new TrinoException(DATAINSIGHT_INVALID_METADATA,
                        format("A column, (%s) cannot be declared as a Trino array and also be rendered as json.", name));
            }

            switch (type) {
                case "date":
                    List<String> formats = ImmutableList.of();
                    if (value.has("format")) {
                        formats = Arrays.asList(value.get("format").asText().split("\\|\\|"));
                    }
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.DateTimeType(formats)));
                    break;
                case "scaled_float":
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.ScaledFloatType(value.get("scaling_factor").asDouble())));
                    break;
                case "nested":
                case "object":
                    if (value.has("properties")) {
                        result.add(new IndexMetadata.Field(asRawJson, isArray, name, parseType(value.get("properties"), metaNode)));
                    } else {
                        LOG.debug("Ignoring empty object field: %s", name);
                    }
                    break;

                default:
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.PrimitiveType(type)));
            }
        }

        return new IndexMetadata.ObjectType(result.build());
    }

    private JsonNode nullSafeNode(JsonNode jsonNode, String name) {
        if (jsonNode == null || jsonNode.isNull() || jsonNode.get(name) == null) {
            return NullNode.getInstance();
        }
        return jsonNode.get(name);
    }


    public DatainsightRecord executeQuery(String index, String query) {


        String path = format("/%s/_search", index);

        return doRequest("POST", path, Optional.of(query), body -> {
                    try {
                        JsonNode root = OBJECT_MAPPER.readTree(body);
                        if (root.has("error")) {
                            JsonNode error = root.get("error");
                            throw new TrinoException(DATAINSIGHT_QUERY_FAILURE,
                                    String.format("error index:%s type:%s,reason:%s ", error.get("index").asText(),
                                            root.get("type").asText(), root.get("reason").asText()));
                        }
                        if (root.has("hits")) {
                            JsonNode hits = root.get("hits").get("hits");
                            if (hits.size() == 0) {
                                return new DatainsightRecord(ImmutableList.of(), ImmutableList.of());
                            }

                            List<Map<String, String>> columnNames = new ArrayList<>();

                            List<List<Object>> values = new ArrayList<>();
                            for (int i = 0; i < hits.size(); i++) {
                                List<Object> row = new ArrayList<>();
                                JsonNode source = hits.get(i).get("_source");

                                Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
                                while (fields.hasNext()) {
                                    Map.Entry<String, JsonNode> field = fields.next();
                                    row.add(field.getValue().asText());
                                }

                                values.add(row);
                            }
                            hits.get(0).get("_source").fields().forEachRemaining(field -> {
                                Map<String, String> column = new HashMap<>();
                                column.put("name", field.getKey());
                                column.put("type", this.getIndexMetadata(index).getSchema().getFields().stream().
                                        filter(f -> f.getName().equals(field.getKey())).findFirst().get().getType().toString());
                                columnNames.add(column);

                            });

                        }


                    } catch (IOException e) {
                        throw new TrinoException(DATAINSIGHT_INVALID_RESPONSE, e);
                    }

                    return null;
                }
        );
    }


    public DatainsightRecord getPluginResult(String pluginName, String query) {

        String path = format("/_plugins/_%s", pluginName.toLowerCase(Locale.ENGLISH));
        LOG.info("openSearch Plugin search,plugin_type:%s,query:`%s`", pluginName, query);
        return doRequest("POST", path, Optional.of(String.format("{\"query\": \"%s\"}", query)), body -> {
                    try {
                        JsonNode root = OBJECT_MAPPER.readTree(body);
                        if (root.get("status").asInt() != 200) {
                            throw new TrinoException(DATAINSIGHT_QUERY_FAILURE,
                                    String.format("error type:%s,reason:%s,details:%s", root.get("type").asText(),
                                            root.get("reason").asText(), root.get("details").asText()));
                        }
                        JsonNode columns = root.get("schema");
                        JsonNode rows = root.get("datarows");
                        List<Map<String, String>> columnNames = new ArrayList<>();
                        for (int i = 0; i < columns.size(); i++) {
                            Map<String, String> column = new HashMap<>();
                            column.put("type", columns.get(i).get("type").asText());
                            String name = columns.get(i).get("alias") == null ? columns.get(i).get("name").asText() : columns.get(i).get("alias").asText();
                            column.put("name", name);
                            columnNames.add(column);
                        }
                        List<List<Object>> values = new ArrayList<>();
                        for (int i = 0; i < rows.size(); i++) {
                            List<Object> row = new ArrayList<>();
                            for (int j = 0; j < rows.get(i).size(); j++) {
                                //根据索引位置，找到对应的类型，并根据其类型进行as_xx固定格式
                                String type = columnNames.get(j).get("type");
                                if (rows.get(i).get(j).isNull()) {
                                    row.add(null);
                                } else if (type.equals("integer")) {
                                    row.add(rows.get(i).get(j).asInt());
                                } else if (type.equals("long")) {
                                    row.add(rows.get(i).get(j).asLong());
                                } else if (type.equals("float")) {
                                    row.add(rows.get(i).get(j).asDouble());
                                } else if (type.equals("double")) {
                                    row.add(rows.get(i).get(j).asDouble());
                                } else if (type.equals("boolean")) {
                                    row.add(rows.get(i).get(j).asBoolean());
                                } else {
                                    row.add(rows.get(i).get(j).asText(""));
                                }

                            }
                            values.add(row);
                        }
                        return new DatainsightRecord(columnNames, values);
                    } catch (IOException e) {
                        throw new TrinoException(DATAINSIGHT_INVALID_RESPONSE, e);
                    }

                }
        );
    }

    public String getIndexesByMongo(Optional<String> streams, String startTime, String endTime) {
        boolean filter;
        if (streams.get() == "" || streams == null || streams.get() == "ALL") {
            //获取全部的索引
            filter = false;
        } else {
            filter = true;
        }

        MongoDatabase db = mongoClient.getDatabase(mongoDb);
        // 给数组元素添加双引号
        List<String> streamList = Arrays.stream(streams.get().split(",")).toList();


        Bson dbFilter;
        if (filter) {
            dbFilter = Filters.in("title", streamList);
        } else {
            dbFilter = Filters.empty();
        }
        Map<String, String> streamTitleIndexSeIdtMap = new HashMap<>();
        List<String> streamIds = new ArrayList<>();

        db.getCollection("streams").find(dbFilter).forEach((Document stream) -> {
            streamTitleIndexSeIdtMap.put(stream.getString("title"), stream.getString("index_set_id"));
            streamIds.add(stream.get("_id").toString());

        });

        List<String> indexNames = new ArrayList<>();
        List<String> indexPrefixes = new ArrayList<>();
        // _id in (index_set_id1, index_set_id2, ...) id是objectid类型，要转化
        Bson indexSetFilter = Filters.in("_id", streamTitleIndexSeIdtMap.values().stream().map(id -> new org.bson.types.ObjectId(id)).toList());
        db.getCollection("index_sets").find(indexSetFilter).forEach((Document indexSet) -> {
                    String indexPrefix = indexSet.getString("index_prefix");
                    indexPrefixes.add(indexPrefix);
                }
        );
        List<String> allIndexNames = new ArrayList<>();

        db.getCollection("index_ranges").find(indexSetFilter).forEach((Document indexRange) -> {
                    String index_name = indexRange.getString("index_name");
                    allIndexNames.add(index_name);
                }
        );
        Document indexRangeFilter = new Document();
        indexRangeFilter.append("stream_ids", new Document("$in", streamIds));
        if (startTime != null && !endTime.isEmpty() && endTime != null && !endTime.isEmpty()) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            LocalDateTime localStartTime = LocalDateTime.parse(startTime, formatter);
            LocalDateTime localEndTime = LocalDateTime.parse(endTime, formatter);
            long startTs = localStartTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            ;
            long endTs = localEndTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            ;
            indexRangeFilter.append("begin", new Document("$lte", endTs));
            indexRangeFilter.append("end", new Document("$gte", startTs));
            db.getCollection("index_ranges").find(indexRangeFilter).forEach((Document indexRange) -> {
                        String indexName = indexRange.getString("index_name");
                        indexNames.add(indexName);
                    }
            );
            for (String indexPrefix : indexPrefixes) {
                String defaultIndexName = String.format("%s_0", indexPrefix);
                if (allIndexNames.contains(defaultIndexName)) {
                    indexNames.add(defaultIndexName);
                }
            }
            } else{

                for (String indexPrefix : indexPrefixes) {
                    indexNames.add(String.format("%s_*", indexPrefix));
                }
            }

            String indexNamesString = String.join(",", indexNames);
            System.out.println(indexNamesString);
            return indexNamesString;
        }

        @Managed
        @Nested
        public TimeStat getSearchStats () {
            return searchStats;
        }

        @Managed
        @Nested
        public TimeStat getNextPageStats () {
            return nextPageStats;
        }

        @Managed
        @Nested
        public TimeStat getCountStats () {
            return countStats;
        }

        @Managed
        @Nested
        public TimeStat getBackpressureStats () {
            return backpressureStats;
        }


        private <T > T
        doRequest(String method, String path, Optional < String > entityString, ResponseHandler < T > handler) {
            String response;
            try {
                String key = cache.generateKey(method + path + (entityString == null ? "" : entityString));
                response = cache.get(key);

                if (response == null) {
                    response = client.performRequest(method, path, entityString);
                    cache.put(key, response, cacheExpireTime);
                    LOG.info(String.format("response api success and set cache ,key:%s，method:%s,path:%s,entity:%s", key, method, path, entityString));
                }

            } catch (IOException e) {
                throw new TrinoException(DATAINSIGHT_CONNECTION_ERROR, e);
            }
            return handler.process(response);
        }


//    private static TrinoException propagate(ResponseException exception) {
//        HttpEntity entity = exception.getResponse().getEntity();
//
//        if (entity != null && entity.getContentType() != null) {
//            try {
//                JsonNode reason = OBJECT_MAPPER.readTree(entity.getContent()).path("error")
//                        .path("root_cause")
//                        .path(0)
//                        .path("reason");
//
//                if (!reason.isMissingNode()) {
//                    throw new TrinoException(DATAINSIGHT_QUERY_FAILURE, reason.asText(), exception);
//                }
//            } catch (IOException e) {
//                TrinoException result = new TrinoException(DATAINSIGHT_QUERY_FAILURE, exception);
//                result.addSuppressed(e);
//                throw result;
//            }
//        }
//
//        throw new TrinoException(DATAINSIGHT_QUERY_FAILURE, exception);
//    }

        @VisibleForTesting
        static Optional<String> extractAddress (String address){
            Matcher matcher = ADDRESS_PATTERN.matcher(address);

            if (!matcher.matches()) {
                return Optional.empty();
            }

            String cname = matcher.group("cname");
            String ip = matcher.group("ip");
            String port = matcher.group("port");

            if (cname != null) {
                return Optional.of(cname + ":" + port);
            }

            return Optional.of(ip + ":" + port);
        }

        public boolean indexExists (String table){
            // TODO: 2023/9/5
            return true;
        }


        private interface ResponseHandler<T> {
            T process(String body);
        }
    }
