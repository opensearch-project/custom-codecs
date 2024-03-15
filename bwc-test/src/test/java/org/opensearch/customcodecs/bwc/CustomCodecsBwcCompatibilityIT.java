/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.customcodecs.bwc;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.customcodecs.bwc.helper.RestHelper;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;

import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;

public class CustomCodecsBwcCompatibilityIT extends OpenSearchRestTestCase {
    private ClusterType CLUSTER_TYPE;
    private String CLUSTER_NAME;
    private static RestClient testUserRestClient = null;

    @Before
    public void testSetup() throws IOException {
        final String bwcsuiteString = System.getProperty("tests.rest.bwcsuite");
        Assume.assumeTrue("Test cannot be run outside the BWC gradle task 'bwcTestSuite' or its dependent tasks", bwcsuiteString != null);
        CLUSTER_TYPE = ClusterType.parse(bwcsuiteString);
        logger.info("Running Test for Cluster Type: {}", CLUSTER_TYPE);
        CLUSTER_NAME = System.getProperty("tests.clustername");
        if (testUserRestClient == null) {
            testUserRestClient = buildClient(super.restClientSettings(), super.getClusterHosts().toArray(new HttpHost[0]));
        }
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureHttpOrHttpsClient(builder, settings);
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    protected void configureHttpOrHttpsClient(RestClientBuilder builder, Settings settings) throws IOException {
        configureClient(builder, settings);

        if (getProtocol().equalsIgnoreCase("https")) {
            final String username = System.getProperty("user");
            if (Strings.isNullOrEmpty(username)) {
                throw new RuntimeException("user name is missing");
            }

            final String password = System.getProperty("password");
            if (Strings.isNullOrEmpty(password)) {
                throw new RuntimeException("password is missing");
            }

            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            final AuthScope anyScope = new AuthScope(null, -1);
            credentialsProvider.setCredentials(anyScope, new UsernamePasswordCredentials(username, password.toCharArray()));

            try {
                final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSslContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build())
                    // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
                    .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                        @Override
                        public TlsDetails create(final SSLEngine sslEngine) {
                            return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                        }
                    })
                    .build();

                builder.setHttpClientConfigCallback(httpClientBuilder -> {
                    final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                        .setMaxConnPerRoute(DEFAULT_MAX_CONN_PER_ROUTE)
                        .setMaxConnTotal(DEFAULT_MAX_CONN_TOTAL)
                        .setTlsStrategy(tlsStrategy)
                        .build();

                    return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setConnectionManager(connectionManager);
                });
            } catch (final NoSuchAlgorithmException | KeyManagementException | KeyStoreException ex) {
                throw new IOException(ex);
            }

        }
    }

    @Override
    protected String getProtocol() {
        return Objects.equals(System.getProperty("https"), "true") ? "https" : "http";
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    /**
     * Tests backward compatibility by created a test user and role with DLS, FLS and masked field settings. Ingests
     * data into a test index and runs a matchAll query against the same.
     */
    public void testDataIngestionAndSearchBackwardsCompatibility() throws Exception {
        String round = System.getProperty("tests.rest.bwcsuite_round");
        String index = "test-custom-codec-index";
        if (round.equals("old")) {
            createIndexIfNotExists(index);
        }
        ingestData(index);
        searchMatchAll(index);
    }

    /**
     * Ingests data into the test index
     *
     * @param index index to ingest data into
     */

    private void ingestData(String index) throws IOException {
        assertTrue(indexExists(index));
        StringBuilder bulkRequestBody = new StringBuilder();
        int numberOfRequests = Randomness.get().nextInt(10);
        while (numberOfRequests-- > 0) {
            for (int i = 0; i < Randomness.get().nextInt(100); i++) {
                Map<String, Map<String, String>> indexRequest = new HashMap<>();
                indexRequest.put("index", new HashMap<>() {
                    {
                        put("_index", index);
                    }
                });

                try (final XContentBuilder contentBuilder = MediaTypeRegistry.JSON.contentBuilder()) {
                    contentBuilder.map(indexRequest);
                    bulkRequestBody.append(contentBuilder.toString() + "\n");
                }

                bulkRequestBody.append(Song.randomSong().asJson() + "\n");
            }
            List<Response> responses = RestHelper.requestAgainstAllNodes(
                testUserRestClient,
                "POST",
                "_bulk?refresh=wait_for",
                RestHelper.toHttpEntity(bulkRequestBody.toString())
            );
            responses.forEach(r -> assertEquals(200, r.getStatusLine().getStatusCode()));
        }
    }

    /**
     * Runs a matchAll query against the test index
     *
     * @param index index to search
     */
    private void searchMatchAll(String index) throws IOException {
        String matchAllQuery = "{\n" + "    \"query\": {\n" + "        \"match_all\": {}\n" + "    }\n" + "}";
        int numberOfRequests = Randomness.get().nextInt(10);
        while (numberOfRequests-- > 0) {
            List<Response> responses = RestHelper.requestAgainstAllNodes(
                testUserRestClient,
                "POST",
                index + "/_search",
                RestHelper.toHttpEntity(matchAllQuery)
            );
            responses.forEach(r -> assertEquals(200, r.getStatusLine().getStatusCode()));
        }
    }

    /**
     * Creates a test index if it does not exist already
     *
     * @param index index to create
     */

    private void createIndexIfNotExists(String index) throws IOException {

        if (indexExists(index)) {
            logger.info("Index {} already created for the domain", index);
            return;
        }

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom("zstd", "zstd_no_dict"))
                .put("index.codec.compression_level", randomIntBetween(1, 6))
                .build()
        );
        ensureGreen(index);
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        IOUtils.close(testUserRestClient);
    }

}
