/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.test.OpenSearchTestCase;

public class Lucene99QatStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testDeflateLucene99QatCodecMode() {
        if (!QatZipperFactory.isQatAvailable()) return;
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_DEFLATE);
        assertEquals(Lucene99QatCodec.Mode.QAT_DEFLATE, lucene99QatStoredFieldsFormat.getMode());
    }

    public void testLz4Lucene99QatCodecMode() {
        if (!QatZipperFactory.isQatAvailable()) return;
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_LZ4);
        assertEquals(Lucene99QatCodec.Mode.QAT_LZ4, lucene99QatStoredFieldsFormat.getMode());
    }

    public void testDeflateLucene99QatCodecModeWithCompressionLevel() {
        if (!QatZipperFactory.isQatAvailable()) return;
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(
            Lucene99QatCodec.Mode.QAT_DEFLATE,
            randomCompressionLevel
        );
        assertEquals(Lucene99QatCodec.Mode.QAT_DEFLATE, lucene99QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene99QatStoredFieldsFormat.getCompressionLevel());
    }

    public void testLz4Lucene99QatCodecModeWithCompressionLevel() {
        if (!QatZipperFactory.isQatAvailable()) return;
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(
            Lucene99QatCodec.Mode.QAT_LZ4,
            randomCompressionLevel
        );
        assertEquals(Lucene99QatCodec.Mode.QAT_LZ4, lucene99QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene99QatStoredFieldsFormat.getCompressionLevel());
    }

    public void testDeflateCompressionModes() {
        if (!QatZipperFactory.isQatAvailable()) return;
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_DEFLATE);
        assertTrue(lucene99QatStoredFieldsFormat.getCompressionMode() instanceof QatDeflateCompressionMode);
    }

    public void testLz4CompressionModes() {
        if (!QatZipperFactory.isQatAvailable()) return;
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_LZ4);
        assertTrue(lucene99QatStoredFieldsFormat.getCompressionMode() instanceof QatLz4CompressionMode);
    }
}
