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

/** Test QAT LZ4 */
public class QatLz4CompressorTests extends AbstractCompressorTests {

    @Override
    Compressor compressor() {
        if (!QatZipperFactory.isQatAvailable()) return null;
        return new QatLz4CompressionMode().newCompressor();
    }

    @Override
    Decompressor decompressor() {
        if (!QatZipperFactory.isQatAvailable()) return null;
        return new QatLz4CompressionMode().newDecompressor();
    }

    @Override
    public void testEmpty() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testEmpty();
    }

    @Override
    public void testShortLiterals() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testShortLiterals();
    }

    @Override
    public void testRandom() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testRandom();
    }

    @Override
    public void testLineDocs() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testLineDocs();
    }

    @Override
    public void testRepetitionsL() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testRepetitionsL();
    }

    @Override
    public void testRepetitionsI() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testRepetitionsI();
    }

    @Override
    public void testRepetitionsS() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testRepetitionsS();
    }

    @Override
    public void testMixed() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.testMixed();
    }

    @Override
    protected void doTest(byte[] bytes) throws IOException {
        if (!QatZipperFactory.isQatAvailable()) return;
        super.doTest(bytes);
    }
}
