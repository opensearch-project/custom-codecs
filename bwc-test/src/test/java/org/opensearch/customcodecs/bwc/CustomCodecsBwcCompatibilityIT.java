/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.customcodecs.bwc;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Randomness;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.customcodecs.bwc.helper.RestHelper;
import org.opensearch.test.rest.OpenSearchRestTestCase;

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
            testUserRestClient = buildClient(
                    super.restClientSettings(),
                    super.getClusterHosts().toArray(new HttpHost[0])
            );
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

            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

            try {
                final SSLContext sslContext = new SSLContextBuilder()
                        .loadTrustMaterial(null, TrustAllStrategy.INSTANCE)
                        .build();

                builder.setHttpClientConfigCallback(httpClientBuilder -> {
                    return httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider)
                            .setSSLContext(sslContext)
                            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
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
        ObjectMapper objectMapper = new ObjectMapper();
        int numberOfRequests = Randomness.get().nextInt(10);
        while (numberOfRequests-- > 0) {
            for (int i = 0; i < Randomness.get().nextInt(100); i++) {
                Map<String, Map<String, String>> indexRequest = new HashMap<>();
                indexRequest.put("index", new HashMap<>() {
                    {
                        put("_index", index);
                    }
                });
                bulkRequestBody.append(objectMapper.writeValueAsString(indexRequest) + "\n");
                bulkRequestBody.append(objectMapper.writeValueAsString(Song.randomSong().asJson()) + "\n");
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
