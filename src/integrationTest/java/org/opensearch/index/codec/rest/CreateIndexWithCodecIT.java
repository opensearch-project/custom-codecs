/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.rest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;

import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.codec.customcodecs.Lucene912QatCodec;
import org.opensearch.index.codec.customcodecs.QatZipperFactory;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
import static org.opensearch.client.RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_DEFLATE_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_LZ4_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_NO_DICT_CODEC;
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
                        randomFrom(Lucene912QatCodec.Mode.QAT_LZ4.getCodec(), Lucene912QatCodec.Mode.QAT_DEFLATE.getCodec(), Lucene912QatCodec.Mode.QAT_ZSTD.getCodec())
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

    /**
     * wipeAllIndices won't work since it cannot delete security index
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }
}
