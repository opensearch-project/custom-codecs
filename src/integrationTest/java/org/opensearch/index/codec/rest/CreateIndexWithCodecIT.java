/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.rest;

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
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.codec.customcodecs.Lucene104QatCodec;
import org.opensearch.index.codec.customcodecs.QatZipperFactory;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;
import static org.opensearch.index.codec.customcodecs.CustomAdditionalCodecs.QAT_DEFLATE_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomAdditionalCodecs.QAT_LZ4_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomAdditionalCodecs.QAT_ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomAdditionalCodecs.ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomAdditionalCodecs.ZSTD_NO_DICT_CODEC;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

public class CreateIndexWithCodecIT extends OpenSearchRestTestCase {
    public void testCreateIndexWithZstdCodec() throws IOException {
        final String index = "custom-codecs-test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(ZSTD_CODEC, ZSTD_NO_DICT_CODEC))
                .put("index.codec.compression_level", randomIntBetween(1, 6))
                .build()
        );

        try {
            ensureGreen(index);
        } finally {
            deleteIndex(index);
        }
    }

    public void testCreateIndexWithQatCodecWithQatHardwareUnavailable() throws IOException {

        assumeThat("Qat library is not available", QatZipperFactory.isQatAvailable(), is(false));
        final String index = "custom-codecs-test-index";

        // creating index
        final ResponseException e = expectThrows(
            ResponseException.class,
            () -> createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.codec", randomFrom(QAT_DEFLATE_CODEC, QAT_LZ4_CODEC, QAT_ZSTD_CODEC))
                    .put("index.codec.compression_level", randomIntBetween(1, 6))
                    .build()
            )
        );
        assertTrue(e.getResponse().toString().contains("400 Bad Request"));
    }

    public void testCreateIndexWithQatSPICodecWithQatHardwareUnavailable() throws IOException {

        assumeThat("Qat library is not available", QatZipperFactory.isQatAvailable(), is(false));
        final String index = "custom-codecs-test-index";

        // creating index
        final ResponseException e = expectThrows(
            ResponseException.class,
            () -> createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(
                        "index.codec",
                        randomFrom(
                            Lucene104QatCodec.Mode.QAT_LZ4.getCodec(),
                            Lucene104QatCodec.Mode.QAT_DEFLATE.getCodec(),
                            Lucene104QatCodec.Mode.QAT_ZSTD.getCodec()
                        )
                    )
                    .put("index.codec.compression_level", randomIntBetween(1, 6))
                    .build()
            )
        );
        assertTrue(e.getResponse().toString().contains("400 Bad Request"));

    }

    public void testCreateIndexWithQatCodec() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));

        final String index = "custom-codecs-test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(QAT_DEFLATE_CODEC, QAT_LZ4_CODEC, QAT_ZSTD_CODEC))
                .put("index.codec.compression_level", randomIntBetween(1, 6))
                .build()
        );

        try {
            ensureGreen(index);
        } finally {
            deleteIndex(index);
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

    /**
     * wipeAllIndices won't work since it cannot delete security index
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }
}
