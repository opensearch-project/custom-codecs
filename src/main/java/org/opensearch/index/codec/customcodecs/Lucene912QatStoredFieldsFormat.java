/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;
import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_QAT_MODE;

/** Stored field format used by pluggable codec */
public class Lucene912QatStoredFieldsFormat extends StoredFieldsFormat {

    /** A key that we use to map to a mode */
    public static final String MODE_KEY = Lucene912QatStoredFieldsFormat.class.getSimpleName() + ".mode";

    private static final int QAT_DEFLATE_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int QAT_DEFLATE_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QAT_DEFLATE_BLOCK_SHIFT = 10;

    private static final int QAT_LZ4_BLOCK_LENGTH = 10 * 8 * 1024;
    private static final int QAT_LZ4_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QAT_LZ4_BLOCK_SHIFT = 10;

    private static final int QAT_ZSTD_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int QAT_ZSTD_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QAT_ZSTD_BLOCK_SHIFT = 10;

    private final QatCompressionMode qatCompressionMode;
    private final Lucene912QatCodec.Mode mode;

    /** default constructor */
    public Lucene912QatStoredFieldsFormat() {
        this(Lucene912QatCodec.DEFAULT_COMPRESSION_MODE, DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD
     */
    public Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode mode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD
     * @param compressionLevel The compression level for the mode.
     */
    public Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode mode, int compressionLevel) {
        this(mode, compressionLevel, () -> { return DEFAULT_QAT_MODE; });
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD
     * @param supplier a supplier for QAT acceleration mode.
     */
    public Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode mode, Supplier<QatZipper.Mode> supplier) {
        this(mode, DEFAULT_COMPRESSION_LEVEL, supplier);
    }

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD
     * @param compressionLevel The compression level for the mode.
     * @param supplier a supplier for QAT acceleration mode.
     */
    public Lucene912QatStoredFieldsFormat(Lucene912QatCodec.Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        this.mode = Objects.requireNonNull(mode);
        qatCompressionMode = new QatCompressionMode(getAlgorithm(mode), compressionLevel, supplier);
    }

    /**
     * Returns a {@link StoredFieldsReader} to load stored fields.
     *
     * @param directory The index directory.
     * @param si The SegmentInfo that stores segment information.
     * @param fn The fieldInfos.
     * @param context The IOContext that holds additional details on the merge/search context.
     */
    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        if (si.getAttribute(MODE_KEY) != null) {
            String value = si.getAttribute(MODE_KEY);
            Lucene912QatCodec.Mode mode = Lucene912QatCodec.Mode.valueOf(value);
            return impl(mode).fieldsReader(directory, si, fn, context);
        } else {
            throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
        }
    }

    /**
     * Returns a {@link StoredFieldsReader} to write stored fields.
     *
     * @param directory The index directory.
     * @param si The SegmentInfo that stores segment information.
     * @param context The IOContext that holds additional details on the merge/search context.
     */
    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                "found existing value for " + MODE_KEY + " for segment: " + si.name + " old = " + previous + ", new = " + mode.name()
            );
        }
        return impl(mode).fieldsWriter(directory, si, context);
    }

    private StoredFieldsFormat impl(Lucene912QatCodec.Mode mode) {
        switch (mode) {
            case QAT_LZ4:
                return getQatCompressingStoredFieldsFormat(
                    "QatStoredFieldsLz4",
                    qatCompressionMode,
                    QAT_LZ4_BLOCK_LENGTH,
                    QAT_LZ4_MAX_DOCS_PER_BLOCK,
                    QAT_LZ4_BLOCK_SHIFT
                );
            case QAT_DEFLATE:
                return getQatCompressingStoredFieldsFormat(
                    "QatStoredFieldsDeflate",
                    qatCompressionMode,
                    QAT_DEFLATE_BLOCK_LENGTH,
                    QAT_DEFLATE_MAX_DOCS_PER_BLOCK,
                    QAT_DEFLATE_BLOCK_SHIFT
                );
            case QAT_ZSTD:
                return getQatCompressingStoredFieldsFormat(
                    "QatStoredFieldsZstd",
                    qatCompressionMode,
                    QAT_ZSTD_BLOCK_LENGTH,
                    QAT_ZSTD_MAX_DOCS_PER_BLOCK,
                    QAT_ZSTD_BLOCK_SHIFT
                );
            default:
                throw new IllegalStateException("Unsupported compression mode: " + mode);
        }
    }

    private StoredFieldsFormat getQatCompressingStoredFieldsFormat(
        String formatName,
        CompressionMode compressionMode,
        int blockSize,
        int maxDocs,
        int blockShift
    ) {
        return new Lucene90CompressingStoredFieldsFormat(formatName, compressionMode, blockSize, maxDocs, blockShift);
    }

    /**
     * Gets the mode of compression.
     *
     * @return either QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD
     */
    public Lucene912QatCodec.Mode getMode() {
        return mode;
    }

    /**
     *
     * @return the CompressionMode instance.
     */
    public QatCompressionMode getCompressionMode() {
        return qatCompressionMode;
    }

    /**
     * Returns {@link QatZipper.Algorithm} instance that corresponds codec's {@link Lucene912QatCodec.Mode mode}
     * @param mode codec's {@link Lucene912QatCodec.Mode mode}
     * @return the {@link QatZipper.Algorithm} instance that corresponds codec's {@link Lucene912QatCodec.Mode mode}
     */
    private static QatZipper.Algorithm getAlgorithm(Lucene912QatCodec.Mode mode) {
        switch (mode) {
            case QAT_LZ4:
                return QatZipper.Algorithm.LZ4;
            case QAT_DEFLATE:
                return QatZipper.Algorithm.DEFLATE;
            case QAT_ZSTD:
                return QatZipper.Algorithm.ZSTD;
            default:
                throw new IllegalStateException("Unsupported compression mode: " + mode);
        }
    }
}
