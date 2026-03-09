/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.test.OpenSearchTestCase;

public class Lucene104CustomStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDefaultLucene104CustomCodecMode() {
        Lucene104CustomStoredFieldsFormat lucene104CustomStoredFieldsFormat = new Lucene104CustomStoredFieldsFormat();
        assertEquals(Lucene104CustomCodec.Mode.ZSTD, lucene104CustomStoredFieldsFormat.getMode());
    }

    public void testZstdNoDictLucene104CustomCodecMode() {
        Lucene104CustomStoredFieldsFormat lucene104CustomStoredFieldsFormat = new Lucene104CustomStoredFieldsFormat(
            Lucene104CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertEquals(Lucene104CustomCodec.Mode.ZSTD_NO_DICT, lucene104CustomStoredFieldsFormat.getMode());
    }

    public void testZstdModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene104CustomStoredFieldsFormat lucene104CustomStoredFieldsFormat = new Lucene104CustomStoredFieldsFormat(
            Lucene104CustomCodec.Mode.ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene104CustomCodec.Mode.ZSTD, lucene104CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene104CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictLucene104CustomCodecModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene104CustomStoredFieldsFormat lucene104CustomStoredFieldsFormat = new Lucene104CustomStoredFieldsFormat(
            Lucene104CustomCodec.Mode.ZSTD_NO_DICT,
            randomCompressionLevel
        );
        assertEquals(Lucene104CustomCodec.Mode.ZSTD_NO_DICT, lucene104CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene104CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testCompressionModes() {
        Lucene104CustomStoredFieldsFormat lucene104CustomStoredFieldsFormat = new Lucene104CustomStoredFieldsFormat();
        assertTrue(lucene104CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdCompressionMode);
    }

    public void testZstdNoDictCompressionModes() {
        Lucene104CustomStoredFieldsFormat lucene104CustomStoredFieldsFormat = new Lucene104CustomStoredFieldsFormat(
            Lucene104CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertTrue(lucene104CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdNoDictCompressionMode);
    }

}
