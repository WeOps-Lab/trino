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

import com.google.common.base.Stopwatch;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.trino.spi.TrinoException;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.*;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.datainsight.DatainsightErrorCode.DATAINSIGHT_CONNECTION_ERROR;
import static io.trino.plugin.datainsight.DatainsightErrorCode.DATAINSIGHT_INVALID_RESPONSE;
import static java.util.Objects.requireNonNull;

public class RestClient
        implements Closeable {
    private static final Logger log = Logger.get(RestClient.class);
    private final HttpClient client;
    private final String endpoint;
    private final ThreadLocal<Stopwatch> stopwatch = ThreadLocal.withInitial(Stopwatch::createUnstarted);

    private String encodeAuth = "";


    public RestClient(DatainsightConfig config) {
        // 识别配置文件中的opensearchUri，判断是否为https
        try {
            if (config.getOpensearchUri().startsWith("https")) {
                SSLContext sslContext = SSLContexts.custom()
                        .loadTrustMaterial(null, new TrustSelfSignedStrategy())
                        .build();
                SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
                client = HttpClients.custom()
                        .setSSLSocketFactory(sslSocketFactory)
                        .build();

            } else {
                client = HttpClients.createDefault();
            }
            if (config.getOpensearchUsername() != "" && config.getOpensearchPassword() != "") {
                encodeAuth = Base64.getEncoder().encodeToString((config.getOpensearchUsername() + ":" + config.getOpensearchPassword()).getBytes(Charset.defaultCharset()));
            }

        } catch (Exception e) {
            throw new TrinoException(DATAINSIGHT_CONNECTION_ERROR, e);
        }
        endpoint = config.getOpensearchUri();
    }


    public HttpClient getClient() {
        return client;
    }


    @Override
    public void close() throws IOException {

    }

    public String performRequest(String method, String path, Optional<String> entityString) throws IOException {
        String url = endpoint + path;
        HttpRequestBase request;
        HttpEntity entity;
        if (entityString.isPresent()) {
            entity = new StringEntity(entityString.get(), ContentType.APPLICATION_JSON);
        } else {
            entity = null;
        }
        ;
        switch (method.toUpperCase(Locale.ENGLISH)) {
            case "GET":
                request = new HttpGet(url);
                break;
            case "POST":
                request = new HttpPost(url);
                if (entity != null) {
                    ((HttpPost) request).setEntity(entity);
                }
                break;
            case "PUT":
                request = new HttpPut(url);
                if (entity != null) {
                    ((HttpPut) request).setEntity(entity);
                }
                break;
            case "DELETE":
                request = new HttpDelete(url);
                break;
            default:
                throw new IllegalArgumentException(String.format("Unsupported HTTP method:%s url:%s", method, url));
        }
        if (encodeAuth != "") {
            request.setHeader("Authorization", "Basic " + encodeAuth);
        }
        try (CloseableHttpResponse httpResponse = (CloseableHttpResponse) client.execute(request)) {
            HttpEntity responseEntity = httpResponse.getEntity();
            if (responseEntity != null) {
                return EntityUtils.toString(responseEntity);
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new TrinoException(DATAINSIGHT_INVALID_RESPONSE, e);
        }
    }
}
