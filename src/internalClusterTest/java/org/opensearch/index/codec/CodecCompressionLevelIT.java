/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.core.util.Throwables;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.codec.customcodecs.CustomCodecPlugin;
import org.opensearch.index.codec.customcodecs.QatZipperFactory;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_DEFLATE_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_LZ4_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_NO_DICT_CODEC;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class CodecCompressionLevelIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(CustomCodecPlugin.class);
    }

    public void testLuceneCodecsCreateIndexWithCompressionLevel() {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        assertThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                    .put("index.codec.compression_level", randomIntBetween(1, 6))
                    .build()
            )
        );

        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                .build()
        );
        ensureGreen(index);
    }

    public void testZStandardCodecsCreateIndexWithCompressionLevel() {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

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

        ensureGreen(index);
    }

    public void testQatCodecsCreateIndexWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

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

        ensureGreen(index);
    }

    public void testZStandardToLuceneCodecsWithCompressionLevel() throws ExecutionException, InterruptedException {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

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
        ensureGreen(index);

        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(1));

        Throwable executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder().put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                    )
                )
                .get()
        );

        Throwable rootCause = Throwables.getRootCause(executionException);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertTrue(rootCause.getMessage().startsWith("Compression level cannot be set"));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                            .put("index.codec.compression_level", (String) null)
                    )
                )
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index).setWaitForActiveShards(1));
        ensureGreen(index);
    }

    public void testQatToLuceneCodecsWithCompressionLevel() throws ExecutionException, InterruptedException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

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
        ensureGreen(index);

        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(1));

        Throwable executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder().put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                    )
                )
                .get()
        );

        Throwable rootCause = Throwables.getRootCause(executionException);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertTrue(rootCause.getMessage().startsWith("Compression level cannot be set"));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                            .put("index.codec.compression_level", (String) null)
                    )
                )
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index).setWaitForActiveShards(1));
        ensureGreen(index);
    }

    public void testLuceneToZStandardCodecsWithCompressionLevel() throws ExecutionException, InterruptedException {

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                .build()
        );
        ensureGreen(index);

        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(1));

        Throwable executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                            .put("index.codec.compression_level", randomIntBetween(1, 6))
                    )
                )
                .get()
        );

        Throwable rootCause = Throwables.getRootCause(executionException);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertTrue(rootCause.getMessage().startsWith("Compression level cannot be set"));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(ZSTD_CODEC, ZSTD_NO_DICT_CODEC))
                            .put("index.codec.compression_level", randomIntBetween(1, 6))
                    )
                )
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index).setWaitForActiveShards(1));
        ensureGreen(index);
    }

    public void testLuceneToQatCodecsWithCompressionLevel() throws ExecutionException, InterruptedException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));

        internalCluster().ensureAtLeastNumDataNodes(1);
        final String index = "test-index";

        // creating index
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                .build()
        );
        ensureGreen(index);

        assertAcked(client().admin().indices().prepareClose(index).setWaitForActiveShards(1));

        Throwable executionException = expectThrows(
            ExecutionException.class,
            () -> client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
                            .put("index.codec.compression_level", randomIntBetween(1, 6))
                    )
                )
                .get()
        );

        Throwable rootCause = Throwables.getRootCause(executionException);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertTrue(rootCause.getMessage().startsWith("Compression level cannot be set"));

        assertAcked(
            client().admin()
                .indices()
                .updateSettings(
                    new UpdateSettingsRequest(index).settings(
                        Settings.builder()
                            .put("index.codec", randomFrom(QAT_DEFLATE_CODEC, QAT_LZ4_CODEC, QAT_ZSTD_CODEC))
                            .put("index.codec.compression_level", randomIntBetween(1, 6))
                    )
                )
                .get()
        );

        assertAcked(client().admin().indices().prepareOpen(index).setWaitForActiveShards(1));
        ensureGreen(index);
    }
}
