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

public class Lucene104QatStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testLz4Lucene104QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(Lucene104QatCodec.Mode.QAT_LZ4);
        assertEquals(Lucene104QatCodec.Mode.QAT_LZ4, lucene104QatStoredFieldsFormat.getMode());
    }

    public void testDeflateLucene104QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(
            Lucene104QatCodec.Mode.QAT_DEFLATE
        );
        assertEquals(Lucene104QatCodec.Mode.QAT_DEFLATE, lucene104QatStoredFieldsFormat.getMode());
    }

    public void testZstdLucene104QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(Lucene104QatCodec.Mode.QAT_ZSTD);
        assertEquals(Lucene104QatCodec.Mode.QAT_ZSTD, lucene104QatStoredFieldsFormat.getMode());
    }

    public void testLz4Lucene104QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(
            Lucene104QatCodec.Mode.QAT_LZ4,
            randomCompressionLevel
        );
        assertEquals(Lucene104QatCodec.Mode.QAT_LZ4, lucene104QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene104QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testDeflateLucene104QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(
            Lucene104QatCodec.Mode.QAT_DEFLATE,
            randomCompressionLevel
        );
        assertEquals(Lucene104QatCodec.Mode.QAT_DEFLATE, lucene104QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene104QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testZstdLucene104QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(
            Lucene104QatCodec.Mode.QAT_ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene104QatCodec.Mode.QAT_ZSTD, lucene104QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene104QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testLz4CompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(Lucene104QatCodec.Mode.QAT_LZ4);
        assertTrue(lucene104QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testDeflateCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(
            Lucene104QatCodec.Mode.QAT_DEFLATE
        );
        assertTrue(lucene104QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testZstdCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene104QatStoredFieldsFormat lucene104QatStoredFieldsFormat = new Lucene104QatStoredFieldsFormat(Lucene104QatCodec.Mode.QAT_ZSTD);
        assertTrue(lucene104QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }
}
