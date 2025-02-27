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

public class Lucene101QatStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testLz4Lucene101QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene101QatStoredFieldsFormat lucene101QatStoredFieldsFormat = new Lucene101QatStoredFieldsFormat(Lucene101QatCodec.Mode.QAT_LZ4);
        assertEquals(Lucene101QatCodec.Mode.QAT_LZ4, lucene101QatStoredFieldsFormat.getMode());
    }

    public void testDeflateLucene101QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene101QatStoredFieldsFormat lucene101QatStoredFieldsFormat = new Lucene101QatStoredFieldsFormat(
            Lucene101QatCodec.Mode.QAT_DEFLATE
        );
        assertEquals(Lucene101QatCodec.Mode.QAT_DEFLATE, lucene101QatStoredFieldsFormat.getMode());
    }

    public void testLz4Lucene101QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene101QatStoredFieldsFormat lucene101QatStoredFieldsFormat = new Lucene101QatStoredFieldsFormat(
            Lucene101QatCodec.Mode.QAT_LZ4,
            randomCompressionLevel
        );
        assertEquals(Lucene101QatCodec.Mode.QAT_LZ4, lucene101QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene101QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testDeflateLucene101QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene101QatStoredFieldsFormat lucene101QatStoredFieldsFormat = new Lucene101QatStoredFieldsFormat(
            Lucene101QatCodec.Mode.QAT_DEFLATE,
            randomCompressionLevel
        );
        assertEquals(Lucene101QatCodec.Mode.QAT_DEFLATE, lucene101QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene101QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testLz4CompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene101QatStoredFieldsFormat lucene101QatStoredFieldsFormat = new Lucene101QatStoredFieldsFormat(Lucene101QatCodec.Mode.QAT_LZ4);
        assertTrue(lucene101QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testDeflateCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene101QatStoredFieldsFormat lucene101QatStoredFieldsFormat = new Lucene101QatStoredFieldsFormat(
            Lucene101QatCodec.Mode.QAT_DEFLATE
        );
        assertTrue(lucene101QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }
}
