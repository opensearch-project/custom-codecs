/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

public class Lucene912QatStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testLz4Lucene912QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode.QAT_LZ4);
        assertEquals(Lucene912QatCodec.Mode.QAT_LZ4, lucene912QatStoredFieldsFormat.getMode());
    }

    public void testDeflateLucene912QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(
            Lucene912QatCodec.Mode.QAT_DEFLATE
        );
        assertEquals(Lucene912QatCodec.Mode.QAT_DEFLATE, lucene912QatStoredFieldsFormat.getMode());
    }

    public void testZstdLucene912QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode.QAT_ZSTD);
        assertEquals(Lucene912QatCodec.Mode.QAT_ZSTD, lucene912QatStoredFieldsFormat.getMode());
    }

    public void testLz4Lucene912QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(
            Lucene912QatCodec.Mode.QAT_LZ4,
            randomCompressionLevel
        );
        assertEquals(Lucene912QatCodec.Mode.QAT_LZ4, lucene912QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene912QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testDeflateLucene912QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(
            Lucene912QatCodec.Mode.QAT_DEFLATE,
            randomCompressionLevel
        );
        assertEquals(Lucene912QatCodec.Mode.QAT_DEFLATE, lucene912QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene912QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testZstdLucene912QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(
            Lucene912QatCodec.Mode.QAT_ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene912QatCodec.Mode.QAT_ZSTD, lucene912QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene912QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testLz4CompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode.QAT_LZ4);
        assertTrue(lucene912QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testDeflateCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(
            Lucene912QatCodec.Mode.QAT_DEFLATE
        );
        assertTrue(lucene912QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testZstdCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene912QatStoredFieldsFormat lucene912QatStoredFieldsFormat = new Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode.QAT_ZSTD);
        assertTrue(lucene912QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }
}
