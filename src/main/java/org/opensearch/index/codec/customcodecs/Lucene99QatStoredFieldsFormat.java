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

/** Stored field format used by pluggable codec */
public class Lucene99QatStoredFieldsFormat extends StoredFieldsFormat {

    /** A key that we use to map to a mode */
    public static final String MODE_KEY = Lucene99QatStoredFieldsFormat.class.getSimpleName() + ".mode";

    private static final int QAT_DEFLATE_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int QAT_DEFLATE_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QAT_DEFLATE_BLOCK_SHIFT = 10;

    private static final int QAT_LZ4_BLOCK_LENGTH = 10 * 8 * 1024;
    private static final int QAT_LZ4_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QATLZ4_BLOCK_SHIFT = 10;

    private final CompressionMode qatDeflateCompressionMode;
    private final CompressionMode qatLz4CompressionMode;

    private final Lucene99QatCodec.Mode mode;
    private final int compressionLevel;

    /** default constructor */
    public Lucene99QatStoredFieldsFormat() {
        this(Lucene99QatCodec.Mode.QAT_DEFLATE, Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents QAT_DEFLATE or QAT_LZ4
     */
    public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode) {
        this(mode, Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents QAT_DEFLATE or QAT_LZ4
     * @param supplier a supplier for QAT acceleration mode.
     */
    public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode, Supplier<QatZipper.Mode> supplier) {
        this(mode, Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL, supplier);
    }

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents QAT_DEFLATE or QAT_LZ4
     * @param compressionLevel The compression level for the mode.
     */
    public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode, int compressionLevel) {
        this(mode, compressionLevel, () -> { return Lucene99QatCodec.DEFAULT_QAT_MODE; });
    }

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents QAT_DEFLATE or QAT_LZ4
     * @param compressionLevel The compression level for the mode.
     * @param supplier a supplier for QAT acceleration mode.
     */
    public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        this.mode = Objects.requireNonNull(mode);
        this.compressionLevel = compressionLevel;
        qatDeflateCompressionMode = new QatDeflateCompressionMode(compressionLevel, supplier);
        qatLz4CompressionMode = new QatLz4CompressionMode(compressionLevel, supplier);
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
            Lucene99QatCodec.Mode mode = Lucene99QatCodec.Mode.valueOf(value);
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

    StoredFieldsFormat impl(Lucene99QatCodec.Mode mode) {
        switch (mode) {
            case QAT_DEFLATE:
                return getQatCompressingStoredFieldsFormat(
                    "QatStoredFieldsDeflate",
                    this.qatDeflateCompressionMode,
                    QAT_DEFLATE_BLOCK_LENGTH,
                    QAT_DEFLATE_MAX_DOCS_PER_BLOCK,
                    QAT_DEFLATE_BLOCK_SHIFT
                );
            case QAT_LZ4:
                return getQatCompressingStoredFieldsFormat(
                    "QatStoredFieldsLz4",
                    this.qatLz4CompressionMode,
                    QAT_LZ4_BLOCK_LENGTH,
                    QAT_DEFLATE_MAX_DOCS_PER_BLOCK,
                    QAT_DEFLATE_BLOCK_SHIFT
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

    public Lucene99QatCodec.Mode getMode() {
        return mode;
    }

    /** Returns the compression level. */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    public CompressionMode getCompressionMode() {
        return mode == Lucene99QatCodec.Mode.QAT_DEFLATE ? qatDeflateCompressionMode : qatLz4CompressionMode;
    }
}
