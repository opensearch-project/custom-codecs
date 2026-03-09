/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs.lucene103;

import org.opensearch.index.codec.customcodecs.ZstdCompressionMode;
import org.opensearch.index.codec.customcodecs.ZstdNoDictCompressionMode;
import org.opensearch.test.OpenSearchTestCase;

public class Lucene103CustomStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDefaultLucene101CustomCodecMode() {
        Lucene103CustomStoredFieldsFormat lucene103CustomStoredFieldsFormat = new Lucene103CustomStoredFieldsFormat();
        assertEquals(Lucene103CustomCodec.Mode.ZSTD, lucene103CustomStoredFieldsFormat.getMode());
    }

    public void testZstdNoDictLucene101CustomCodecMode() {
        Lucene103CustomStoredFieldsFormat lucene103CustomStoredFieldsFormat = new Lucene103CustomStoredFieldsFormat(
            Lucene103CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertEquals(Lucene103CustomCodec.Mode.ZSTD_NO_DICT, lucene103CustomStoredFieldsFormat.getMode());
    }

    public void testZstdModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene103CustomStoredFieldsFormat lucene103CustomStoredFieldsFormat = new Lucene103CustomStoredFieldsFormat(
            Lucene103CustomCodec.Mode.ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene103CustomCodec.Mode.ZSTD, lucene103CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene103CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictLucene101CustomCodecModeWithCompressionLevel() {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene103CustomStoredFieldsFormat lucene103CustomStoredFieldsFormat = new Lucene103CustomStoredFieldsFormat(
            Lucene103CustomCodec.Mode.ZSTD_NO_DICT,
            randomCompressionLevel
        );
        assertEquals(Lucene103CustomCodec.Mode.ZSTD_NO_DICT, lucene103CustomStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene103CustomStoredFieldsFormat.getCompressionLevel());
    }

    public void testCompressionModes() {
        Lucene103CustomStoredFieldsFormat lucene103CustomStoredFieldsFormat = new Lucene103CustomStoredFieldsFormat();
        assertTrue(lucene103CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdCompressionMode);
    }

    public void testZstdNoDictCompressionModes() {
        Lucene103CustomStoredFieldsFormat lucene103CustomStoredFieldsFormat = new Lucene103CustomStoredFieldsFormat(
            Lucene103CustomCodec.Mode.ZSTD_NO_DICT
        );
        assertTrue(lucene103CustomStoredFieldsFormat.getCompressionMode() instanceof ZstdNoDictCompressionMode);
    }

}
