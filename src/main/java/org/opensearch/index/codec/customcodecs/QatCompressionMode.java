/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;
import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_QAT_MODE;

/** QatCompressionMode offers QAT_LZ4, QAT_DEFLATE, and QAT_ZSTD compressors. */
public class QatCompressionMode extends CompressionMode {

    private static final int NUM_SUB_BLOCKS = 10;

    private final QatZipper.Algorithm algorithm;
    private final int compressionLevel;
    private final Supplier<QatZipper.Mode> supplier;

    /** default constructor */
    protected QatCompressionMode() {
        this(QatZipper.Algorithm.LZ4, DEFAULT_COMPRESSION_LEVEL, () -> { return DEFAULT_QAT_MODE; });
    }

    /**
     * Creates a new instance.
     *
     * @param algorithm The compression algorithm (LZ4, DEFLATE, or ZSTD)
     */
    protected QatCompressionMode(QatZipper.Algorithm algorithm) {
        this(algorithm, DEFAULT_COMPRESSION_LEVEL, () -> { return DEFAULT_QAT_MODE; });
    }

    /**
     * Creates a new instance.
     *
     * @param algorithm The compression algorithm (LZ4, DEFLATE, or ZSTD)
     * @param compressionLevel The compression level to use.
     */
    protected QatCompressionMode(QatZipper.Algorithm algorithm, int compressionLevel) {
        this(algorithm, compressionLevel, () -> { return DEFAULT_QAT_MODE; });
    }

    /**
     * Creates a new instance.
     *
     * @param algorithm The compression algorithm (LZ4, DEFLATE, or ZSTD)
     * @param compressionLevel The compression level to use.
     * @param supplier a supplier for QAT acceleration mode.
     */
    protected QatCompressionMode(QatZipper.Algorithm algorithm, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        this.algorithm = algorithm;
        this.compressionLevel = compressionLevel;
        this.supplier = supplier;
    }

    @Override
    public Compressor newCompressor() {
        return new QatCompressor(algorithm, compressionLevel, supplier.get());
    }

    @Override
    public Decompressor newDecompressor() {
        return new QatDecompressor(algorithm, supplier.get());
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    /** The QatCompressor.  */
    private static final class QatCompressor extends Compressor {

        private byte[] compressedBuffer;
        private final QatZipper qatZipper;

        /** compressor with a given algorithm, compresion level, and execution mode */
        public QatCompressor(QatZipper.Algorithm algorithm, int compressionLevel, QatZipper.Mode qatMode) {
            compressedBuffer = BytesRef.EMPTY_BYTES;
            qatZipper = QatZipperFactory.createInstance(algorithm, compressionLevel, qatMode, QatZipper.PollingMode.PERIODICAL);
        }

        private void compress(byte[] bytes, int offset, int length, DataOutput out) throws IOException {
            assert offset >= 0 : "Offset value must be greater than 0.";

            int blockLength = (length + NUM_SUB_BLOCKS - 1) / NUM_SUB_BLOCKS;
            out.writeVInt(blockLength);

            final int end = offset + length;
            assert end >= 0 : "Buffer read size must be greater than 0.";

            for (int start = offset; start < end; start += blockLength) {
                int l = Math.min(blockLength, end - start);

                if (l == 0) {
                    out.writeVInt(0);
                    return;
                }

                final int maxCompressedLength = qatZipper.maxCompressedLength(l);
                compressedBuffer = ArrayUtil.grow(compressedBuffer, maxCompressedLength);

                int compressedSize = qatZipper.compress(bytes, start, l, compressedBuffer, 0, compressedBuffer.length);
                out.writeVInt(compressedSize);
                out.writeBytes(compressedBuffer, compressedSize);
            }
        }

        @Override
        public void compress(ByteBuffersDataInput buffersInput, DataOutput out) throws IOException {
            final int length = (int) buffersInput.length();
            byte[] bytes = new byte[length];
            buffersInput.readBytes(bytes, 0, length);
            compress(bytes, 0, length, out);
        }

        @Override
        public void close() throws IOException {}
    }

    /** The QatDecompressor */
    private static final class QatDecompressor extends Decompressor {

        private byte[] compressed;
        private final QatZipper qatZipper;
        private final QatZipper.Mode qatMode;
        private final QatZipper.Algorithm algorithm;

        /** decompressor with a given algorithm, compression level, and execution mode */
        public QatDecompressor(QatZipper.Algorithm algorithm, QatZipper.Mode qatMode) {
            this.algorithm = algorithm;
            this.qatMode = qatMode;
            compressed = BytesRef.EMPTY_BYTES;
            qatZipper = QatZipperFactory.createInstance(algorithm, qatMode, QatZipper.PollingMode.PERIODICAL);
        }

        /*resuable decompress function*/
        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            assert offset + length <= originalLength : "Buffer read size must be within limit.";

            if (length == 0) {
                bytes.length = 0;
                return;
            }

            final int blockLength = in.readVInt();
            bytes.offset = bytes.length = 0;
            int offsetInBlock = 0;
            int offsetInBytesRef = offset;

            // Skip unneeded blocks
            while (offsetInBlock + blockLength < offset) {
                final int compressedLength = in.readVInt();
                in.skipBytes(compressedLength);
                offsetInBlock += blockLength;
                offsetInBytesRef -= blockLength;
            }

            // Read blocks that intersect with the interval we need
            while (offsetInBlock < offset + length) {
                final int compressedLength = in.readVInt();
                if (compressedLength == 0) {
                    return;
                }
                compressed = ArrayUtil.grow(compressed, compressedLength);
                in.readBytes(compressed, 0, compressedLength);

                int l = Math.min(blockLength, originalLength - offsetInBlock);
                bytes.bytes = ArrayUtil.grow(bytes.bytes, bytes.length + l);

                final int uncompressed = qatZipper.decompress(compressed, 0, compressedLength, bytes.bytes, bytes.length, l);

                bytes.length += uncompressed;
                offsetInBlock += blockLength;
            }

            bytes.offset = offsetInBytesRef;
            bytes.length = length;

            assert bytes.isValid() : "Decompression output is corrupted.";
        }

        @Override
        public Decompressor clone() {
            return new QatDecompressor(algorithm, qatMode);
        }
    }
}
