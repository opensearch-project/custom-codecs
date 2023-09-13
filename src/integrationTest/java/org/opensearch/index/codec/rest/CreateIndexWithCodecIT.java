/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.rest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_NO_DICT_CODEC;

public class CreateIndexWithCodecIT extends OpenSearchRestTestCase {

    public void testCreateIndexWithZstdCodec() throws IOException {
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
}
