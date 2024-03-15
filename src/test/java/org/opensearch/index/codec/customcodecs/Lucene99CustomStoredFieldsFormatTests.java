/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.test.OpenSearchTestCase;

public class Lucene99CustomStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDefaultLucene99CustomCodecMode() {
        Lucene99CustomStoredFieldsFormat lucene99CustomStoredFieldsFormat = new Lucene99CustomStoredFieldsFormat();
        assertEquals(Lucene99CustomCodec.Mode.ZSTD, lucene99CustomStoredFieldsFormat.getMode());
    }

    public void testZstdNoDictLucene99CustomCodecMode() {
        Lucene99CustomStoredFieldsFormat lucene99CustomStoredFieldsFormat = new Lucene99CustomStoredFieldsFormat(
            Lucene99CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertEquals(Lucene99CustomCodec.Mode.ZSTD_NO_DICT, lucene99CustomStoredFieldsFormat.getMode());
    }

    public void testZstdModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene99CustomStoredFieldsFormat lucene99CustomStoredFieldsFormat = new Lucene99CustomStoredFieldsFormat(
            Lucene99CustomCodec.Mode.ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene99CustomCodec.Mode.ZSTD, lucene99CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene99CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictLucene99CustomCodecModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene99CustomStoredFieldsFormat lucene99CustomStoredFieldsFormat = new Lucene99CustomStoredFieldsFormat(
            Lucene99CustomCodec.Mode.ZSTD_NO_DICT,
            randomCompressionLevel
        );
        assertEquals(Lucene99CustomCodec.Mode.ZSTD_NO_DICT, lucene99CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene99CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testCompressionModes() {
        Lucene99CustomStoredFieldsFormat lucene99CustomStoredFieldsFormat = new Lucene99CustomStoredFieldsFormat();
        assertTrue(lucene99CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdCompressionMode);
    }

    public void testZstdNoDictCompressionModes() {
        Lucene99CustomStoredFieldsFormat lucene99CustomStoredFieldsFormat = new Lucene99CustomStoredFieldsFormat(
            Lucene99CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertTrue(lucene99CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdNoDictCompressionMode);
    }

}
