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

public class Lucene99QatStoredFieldsFormatTests extends OpenSearchTestCase {

    public void testLz4Lucene99QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_LZ4);
        assertEquals(Lucene99QatCodec.Mode.QAT_LZ4, lucene99QatStoredFieldsFormat.getMode());
    }

    public void testDeflateLucene99QatCodecMode() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_DEFLATE);
        assertEquals(Lucene99QatCodec.Mode.QAT_DEFLATE, lucene99QatStoredFieldsFormat.getMode());
    }

    public void testLz4Lucene99QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(
            Lucene99QatCodec.Mode.QAT_LZ4,
            randomCompressionLevel
        );
        assertEquals(Lucene99QatCodec.Mode.QAT_LZ4, lucene99QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene99QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testDeflateLucene99QatCodecModeWithCompressionLevel() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(
            Lucene99QatCodec.Mode.QAT_DEFLATE,
            randomCompressionLevel
        );
        assertEquals(Lucene99QatCodec.Mode.QAT_DEFLATE, lucene99QatStoredFieldsFormat.getMode());
        assertEquals(randomCompressionLevel, lucene99QatStoredFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testLz4CompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_LZ4);
        assertTrue(lucene99QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }

    public void testDeflateCompressionModes() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Lucene99QatStoredFieldsFormat lucene99QatStoredFieldsFormat = new Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode.QAT_DEFLATE);
        assertTrue(lucene99QatStoredFieldsFormat.getCompressionMode() instanceof QatCompressionMode);
    }
}
