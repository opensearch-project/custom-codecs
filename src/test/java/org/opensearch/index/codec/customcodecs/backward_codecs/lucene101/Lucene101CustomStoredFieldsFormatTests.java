/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs.lucene101;

import org.opensearch.index.codec.customcodecs.ZstdCompressionMode;
import org.opensearch.index.codec.customcodecs.ZstdNoDictCompressionMode;
import org.opensearch.test.OpenSearchTestCase;

public class Lucene101CustomStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDefaultLucene101CustomCodecMode() {
        Lucene101CustomStoredFieldsFormat lucene101CustomStoredFieldsFormat = new Lucene101CustomStoredFieldsFormat();
        assertEquals(Lucene101CustomCodec.Mode.ZSTD, lucene101CustomStoredFieldsFormat.getMode());
    }

    public void testZstdNoDictLucene101CustomCodecMode() {
        Lucene101CustomStoredFieldsFormat lucene101CustomStoredFieldsFormat = new Lucene101CustomStoredFieldsFormat(
            Lucene101CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertEquals(Lucene101CustomCodec.Mode.ZSTD_NO_DICT, lucene101CustomStoredFieldsFormat.getMode());
    }

    public void testZstdModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene101CustomStoredFieldsFormat lucene101CustomStoredFieldsFormat = new Lucene101CustomStoredFieldsFormat(
            Lucene101CustomCodec.Mode.ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene101CustomCodec.Mode.ZSTD, lucene101CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene101CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictLucene101CustomCodecModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene101CustomStoredFieldsFormat lucene101CustomStoredFieldsFormat = new Lucene101CustomStoredFieldsFormat(
            Lucene101CustomCodec.Mode.ZSTD_NO_DICT,
            randomCompressionLevel
        );
        assertEquals(Lucene101CustomCodec.Mode.ZSTD_NO_DICT, lucene101CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene101CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testCompressionModes() {
        Lucene101CustomStoredFieldsFormat lucene101CustomStoredFieldsFormat = new Lucene101CustomStoredFieldsFormat();
        assertTrue(lucene101CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdCompressionMode);
    }

    public void testZstdNoDictCompressionModes() {
        Lucene101CustomStoredFieldsFormat lucene101CustomStoredFieldsFormat = new Lucene101CustomStoredFieldsFormat(
            Lucene101CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertTrue(lucene101CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdNoDictCompressionMode);
    }

}
