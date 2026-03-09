/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs.lucene103;

import org.opensearch.index.codec.customcodecs.QatCompressionMode;
import org.opensearch.index.codec.customcodecs.QatZipperFactory;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

public class Lucene103QatStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testLz4Lucene101QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(Lucene103QatCodec.Mode.QAT_LZ4);
        assertEquals(Lucene103QatCodec.Mode.QAT_LZ4, lucene103QatStoredFieldsFormat.getMode());
    }

    public void testDeflateLucene101QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(
            Lucene103QatCodec.Mode.QAT_DEFLATE
        );
        assertEquals(Lucene103QatCodec.Mode.QAT_DEFLATE, lucene103QatStoredFieldsFormat.getMode());
    }

    public void testZstdLucene101QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(Lucene103QatCodec.Mode.QAT_ZSTD);
        assertEquals(Lucene103QatCodec.Mode.QAT_ZSTD, lucene103QatStoredFieldsFormat.getMode());
    }

    public void testLz4Lucene101QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(
            Lucene103QatCodec.Mode.QAT_LZ4,
            randomCompressionLevel
        );
        assertEquals(Lucene103QatCodec.Mode.QAT_LZ4, lucene103QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene103QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testDeflateLucene101QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(
            Lucene103QatCodec.Mode.QAT_DEFLATE,
            randomCompressionLevel
        );
        assertEquals(Lucene103QatCodec.Mode.QAT_DEFLATE, lucene103QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene103QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testZstdLucene101QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(
            Lucene103QatCodec.Mode.QAT_ZSTD,
            randomCompressionLevel
        );
        assertEquals(Lucene103QatCodec.Mode.QAT_ZSTD, lucene103QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene103QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testLz4CompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(Lucene103QatCodec.Mode.QAT_LZ4);
        assertTrue(lucene103QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testDeflateCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(
            Lucene103QatCodec.Mode.QAT_DEFLATE
        );
        assertTrue(lucene103QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testZstdCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene103QatStoredFieldsFormat lucene103QatStoredFieldsFormat = new Lucene103QatStoredFieldsFormat(Lucene103QatCodec.Mode.QAT_ZSTD);
        assertTrue(lucene103QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }
}
