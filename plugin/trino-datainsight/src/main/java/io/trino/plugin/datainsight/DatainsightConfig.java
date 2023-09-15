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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import io.trino.spi.type.TypeManager;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@DefunctConfig({
        "datainsight.max-hits",
        "datainsight.cluster-name",
        "searchguard.ssl.certificate-format",
        "searchguard.ssl.pemcert-filepath",
        "searchguard.ssl.pemkey-filepath",
        "searchguard.ssl.pemkey-password",
        "searchguard.ssl.pemtrustedcas-filepath",
        "searchguard.ssl.keystore-filepath",
        "searchguard.ssl.keystore-password",
        "searchguard.ssl.truststore-filepath",
        "searchguard.ssl.truststore-password",
        "datainsight.table-description-directory",
        "datainsight.max-request-retries",
        "datainsight.max-request-retry-time"})
public class DatainsightConfig {


    public enum Security {
        AWS,
        PASSWORD,
    }

    private String defaultSchema = "default";
    private int scrollSize = 1_000;
    private Duration scrollTimeout = new Duration(1, MINUTES);
    private Duration requestTimeout = new Duration(10, SECONDS);
    private Duration connectTimeout = new Duration(1, SECONDS);
    private Duration backoffInitDelay = new Duration(500, MILLISECONDS);


    private Duration backoffMaxDelay = new Duration(20, SECONDS);
    private Duration maxRetryTime = new Duration(30, SECONDS);
    private Duration nodeRefreshInterval = new Duration(1, MINUTES);
    private int maxHttpConnections = 25;

    private int cacheExpireTime = 60 * 1000;

    private int httpThreadCount = Runtime.getRuntime().availableProcessors();

    private boolean tlsEnabled;
    private File keystorePath;
    private File trustStorePath;
    private String keystorePassword;
    private String truststorePassword;
    private boolean ignorePublishAddress;
    private boolean verifyHostnames = true;

    private String mongoUri;
    private String mongoDb;
    private String opensearchUri;

    private String opensearchUsername;
    private String opensearchPassword;
    private Security security;


    @NotNull
    public String getDefaultSchema() {
        return defaultSchema;
    }

    @Config("datainsight.default-schema-name")
    @ConfigDescription("Default schema name to use")
    public DatainsightConfig setDefaultSchema(String defaultSchema) {
        this.defaultSchema = defaultSchema;
        return this;
    }

    @NotNull
    @Min(1)
    public int getScrollSize() {
        return scrollSize;
    }

    @Config("datainsight.scroll-size")
    @ConfigDescription("Scroll batch size")
    public DatainsightConfig setScrollSize(int scrollSize) {
        this.scrollSize = scrollSize;
        return this;
    }

    @NotNull
    public Duration getScrollTimeout() {
        return scrollTimeout;
    }

    @Config("datainsight.scroll-timeout")
    @ConfigDescription("Scroll timeout")
    public DatainsightConfig setScrollTimeout(Duration scrollTimeout) {
        this.scrollTimeout = scrollTimeout;
        return this;
    }

    @NotNull
    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    @Config("datainsight.request-timeout")
    @ConfigDescription("datainsight request timeout")
    public DatainsightConfig setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
        return this;
    }

    @NotNull
    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    @Config("datainsight.connect-timeout")
    @ConfigDescription("datainsight connect timeout")
    public DatainsightConfig setConnectTimeout(Duration timeout) {
        this.connectTimeout = timeout;
        return this;
    }

    public String getMongoUri() {
        return mongoUri;
    }

    @Config("datainsight.mongo-uri")
    @ConfigDescription("datainsight mongo uri")
    public DatainsightConfig setMongoUri(String mongoUri) {
        this.mongoUri = mongoUri;
        return this;

    }

    public String getMongoDb() {
        return mongoDb;
    }

    @Config("datainsight.mongo-db")
    @ConfigDescription("datainsight mongo database name")
    public DatainsightConfig setMongoDb(String mongoDb) {
        this.mongoDb = mongoDb;
        return this;

    }
    @Config("datainsight.cache-expire-time")
    @ConfigDescription("datainsight cache expire time")
    public DatainsightConfig setCacheExpireTime(int cacheExpireTime) {
        this.cacheExpireTime = cacheExpireTime;
        return this;

    }

    public int getCacheExpireTime() {
        return cacheExpireTime;
    }



    public String getOpensearchUri() {
        return opensearchUri;
    }

    @Config("datainsight.opensearch-username")
    @ConfigDescription("datainsight opensearch username")
    public DatainsightConfig setOpensearchUsername(String opensearchUsername) {
        this.opensearchUsername = opensearchUsername;
        return this;

    }

    public String getOpensearchUsername() {
        return opensearchUsername;
    }

    @Config("datainsight.opensearch-password")
    @ConfigDescription("datainsight opensearch password")
    public DatainsightConfig setOpensearchPassword(String opensearchPassword) {
        this.opensearchPassword = opensearchPassword;
        return this;

    }

    public String getOpensearchPassword() {
        return opensearchPassword;
    }

    @Config("datainsight.opensearch-uri")
    @ConfigDescription("datainsight opensearch uri")
    public DatainsightConfig setOpensearchUri(String opensearchUri) {
        this.opensearchUri = opensearchUri;
        return this;

    }

    @NotNull
    public Duration getBackoffInitDelay() {
        return backoffInitDelay;
    }

    @Config("datainsight.backoff-init-delay")
    @ConfigDescription("Initial delay to wait between backpressure retries")
    public DatainsightConfig setBackoffInitDelay(Duration backoffInitDelay) {
        this.backoffInitDelay = backoffInitDelay;
        return this;
    }

    @NotNull
    public Duration getBackoffMaxDelay() {
        return backoffMaxDelay;
    }

    @Config("datainsight.backoff-max-delay")
    @ConfigDescription("Maximum delay to wait between backpressure retries")
    public DatainsightConfig setBackoffMaxDelay(Duration backoffMaxDelay) {
        this.backoffMaxDelay = backoffMaxDelay;
        return this;
    }

    @NotNull
    public Duration getMaxRetryTime() {
        return maxRetryTime;
    }

    @Config("datainsight.max-retry-time")
    @ConfigDescription("Maximum timeout in case of multiple retries")
    public DatainsightConfig setMaxRetryTime(Duration maxRetryTime) {
        this.maxRetryTime = maxRetryTime;
        return this;
    }

    @NotNull
    @MinDuration("1ms")
    public Duration getNodeRefreshInterval() {
        return nodeRefreshInterval;
    }

    @Config("datainsight.node-refresh-interval")
    @ConfigDescription("How often to refresh the list of available datainsight nodes")
    public DatainsightConfig setNodeRefreshInterval(Duration nodeRefreshInterval) {
        this.nodeRefreshInterval = nodeRefreshInterval;
        return this;
    }

    @Config("datainsight.max-http-connections")
    @ConfigDescription("Maximum number of persistent HTTP connections to datainsight")
    public DatainsightConfig setMaxHttpConnections(int size) {
        this.maxHttpConnections = size;
        return this;
    }

    @NotNull
    public int getMaxHttpConnections() {
        return maxHttpConnections;
    }

    @Config("datainsight.http-thread-count")
    @ConfigDescription("Number of threads handling HTTP connections to datainsight")
    public DatainsightConfig setHttpThreadCount(int count) {
        this.httpThreadCount = count;
        return this;
    }

    @NotNull
    public int getHttpThreadCount() {
        return httpThreadCount;
    }

    public boolean isTlsEnabled() {
        return tlsEnabled;
    }

    @Config("datainsight.tls.enabled")
    public DatainsightConfig setTlsEnabled(boolean tlsEnabled) {
        this.tlsEnabled = tlsEnabled;
        return this;
    }

    public Optional<@FileExists File> getKeystorePath() {
        return Optional.ofNullable(keystorePath);
    }

    @Config("datainsight.tls.keystore-path")
    public DatainsightConfig setKeystorePath(File path) {
        this.keystorePath = path;
        return this;
    }

    public Optional<String> getKeystorePassword() {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("datainsight.tls.keystore-password")
    @ConfigSecuritySensitive
    public DatainsightConfig setKeystorePassword(String password) {
        this.keystorePassword = password;
        return this;
    }

    public Optional<@FileExists File> getTrustStorePath() {
        return Optional.ofNullable(trustStorePath);
    }

    @Config("datainsight.tls.truststore-path")
    public DatainsightConfig setTrustStorePath(File path) {
        this.trustStorePath = path;
        return this;
    }

    public Optional<String> getTruststorePassword() {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("datainsight.tls.truststore-password")
    @ConfigSecuritySensitive
    public DatainsightConfig setTruststorePassword(String password) {
        this.truststorePassword = password;
        return this;
    }

    public boolean isVerifyHostnames() {
        return verifyHostnames;
    }

    @Config("datainsight.tls.verify-hostnames")
    public DatainsightConfig setVerifyHostnames(boolean verify) {
        this.verifyHostnames = verify;
        return this;
    }

    public boolean isIgnorePublishAddress() {
        return ignorePublishAddress;
    }

    @Config("datainsight.ignore-publish-address")
    public DatainsightConfig setIgnorePublishAddress(boolean ignorePublishAddress) {
        this.ignorePublishAddress = ignorePublishAddress;
        return this;
    }

    @NotNull
    public Optional<Security> getSecurity() {
        return Optional.ofNullable(security);
    }

    @Config("datainsight.security")
    public DatainsightConfig setSecurity(Security security) {
        this.security = security;
        return this;
    }
}
