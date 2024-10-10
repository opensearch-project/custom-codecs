/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.test.OpenSearchTestCase;

public class Lucene912CustomStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDefaultLucene912CustomCodecMode() {
        Lucene912CustomStoredFieldsFormat lucene912CustomStoredFieldsFormat = new Lucene912CustomStoredFieldsFormat();
        assertEquals(Lucene912CustomCodec.Mode.ZSTD, lucene912CustomStoredFieldsFormat.getMode());
    }

    public void testZstdNoDictLucene912CustomCodecMode() {
        Lucene912CustomStoredFieldsFormat lucene912CustomStoredFieldsFormat = new Lucene912CustomStoredFieldsFormat(
            Lucene912CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertEquals(Lucene912CustomCodec.Mode.ZSTD_NO_DICT, lucene912CustomStoredFieldsFormat.getMode());
    }

    public void testZstdModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene912CustomStoredFieldsFormat lucene912CustomStoredFieldsFormat = new Lucene912CustomStoredFieldsFormat(
            Lucene912CustomCodec.Mode.ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene912CustomCodec.Mode.ZSTD, lucene912CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene912CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictLucene912CustomCodecModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene912CustomStoredFieldsFormat lucene912CustomStoredFieldsFormat = new Lucene912CustomStoredFieldsFormat(
            Lucene912CustomCodec.Mode.ZSTD_NO_DICT,
            randomCompressionLevel
        );
        assertEquals(Lucene912CustomCodec.Mode.ZSTD_NO_DICT, lucene912CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene912CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testCompressionModes() {
        Lucene912CustomStoredFieldsFormat lucene912CustomStoredFieldsFormat = new Lucene912CustomStoredFieldsFormat();
        assertTrue(lucene912CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdCompressionMode);
    }

    public void testZstdNoDictCompressionModes() {
        Lucene912CustomStoredFieldsFormat lucene912CustomStoredFieldsFormat = new Lucene912CustomStoredFieldsFormat(
            Lucene912CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertTrue(lucene912CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdNoDictCompressionMode);
    }

}
