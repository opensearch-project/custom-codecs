/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;

import java.io.IOException;

import com.intel.qat.QatZipper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

/** Test QAT ZSTD compression */
public class QatZstdCompressorTests extends AbstractCompressorTests {

    @Override
    Compressor compressor() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        return new QatCompressionMode(QatZipper.Algorithm.ZSTD).newCompressor();
    }

    @Override
    Decompressor decompressor() {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        return new QatCompressionMode(QatZipper.Algorithm.ZSTD).newDecompressor();
    }

    @Override
    public void testEmpty() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testEmpty();
    }

    @Override
    public void testShortLiterals() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testShortLiterals();
    }

    @Override
    public void testRandom() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testRandom();
    }

    @Override
    public void testLineDocs() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testLineDocs();
    }

    @Override
    public void testRepetitionsL() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testRepetitionsL();
    }

    @Override
    public void testRepetitionsI() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testRepetitionsI();
    }

    @Override
    public void testRepetitionsS() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testRepetitionsS();
    }

    @Override
    public void testMixed() throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.testMixed();
    }

    @Override
    protected void doTest(byte[] bytes) throws IOException {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        super.doTest(bytes);
    }
}
