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
public class Lucene99CustomStoredFieldsFormat extends StoredFieldsFormat {

    /** A key that we use to map to a mode */
    public static final String MODE_KEY = Lucene99CustomStoredFieldsFormat.class.getSimpleName() + ".mode";

    protected static final int ZSTD_BLOCK_LENGTH = 10 * 48 * 1024;
    protected static final int ZSTD_MAX_DOCS_PER_BLOCK = 4096;
    protected static final int ZSTD_BLOCK_SHIFT = 10;

    private static final int QAT_DEFLATE_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int QAT_DEFLATE_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QAT_DEFLATE_BLOCK_SHIFT = 10;

    private static final int QAT_LZ4_BLOCK_LENGTH = 10 * 8 * 1024;
    private static final int QAT_LZ4_MAX_DOCS_PER_BLOCK = 4096;
    private static final int QATLZ4_BLOCK_SHIFT = 10;

    private final CompressionMode zstdCompressionMode;
    private final CompressionMode zstdNoDictCompressionMode;
    private final CompressionMode qatDeflateCompressionMode;
    private final CompressionMode qatLz4CompressionMode;

    private final Lucene99CustomCodec.Mode mode;
    private final int compressionLevel;

    /** default constructor */
    public Lucene99CustomStoredFieldsFormat() {
        this(Lucene99CustomCodec.Mode.ZSTD, Lucene99CustomCodec.DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents ZSTD, ZSTDNODICT, QAT_DEFLATE, or QAT_LZ4
     */
    public Lucene99CustomStoredFieldsFormat(Lucene99CustomCodec.Mode mode) {
        this(mode, Lucene99CustomCodec.DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents ZSTD, ZSTDNODICT, QAT_DEFLATE, or QAT_LZ4
     * @param supplier a supplier for QAT acceleration mode.
     */
    public Lucene99CustomStoredFieldsFormat(Lucene99CustomCodec.Mode mode, Supplier<QatZipper.Mode> supplier) {
        this(mode, Lucene99CustomCodec.DEFAULT_COMPRESSION_LEVEL, supplier);
    }

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents ZSTD, ZSTDNODICT, QAT_DEFLATE, or QAT_LZ4
     * @param compressionLevel The compression level for the mode.
     */
    public Lucene99CustomStoredFieldsFormat(Lucene99CustomCodec.Mode mode, int compressionLevel) {
        this(mode, compressionLevel, () -> { return Lucene99CustomCodec.DEFAULT_QAT_MODE; });
    }

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents ZSTD, ZSTDNODICT, QAT_DEFLATE, or QAT_LZ4
     * @param compressionLevel The compression level for the mode.
     * @param supplier a supplier for QAT acceleration mode.
     */
    public Lucene99CustomStoredFieldsFormat(Lucene99CustomCodec.Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        this.mode = Objects.requireNonNull(mode);
        this.compressionLevel = compressionLevel;
        zstdCompressionMode = new ZstdCompressionMode(compressionLevel);
        zstdNoDictCompressionMode = new ZstdNoDictCompressionMode(compressionLevel);
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
            Lucene99CustomCodec.Mode mode = Lucene99CustomCodec.Mode.valueOf(value);
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

    StoredFieldsFormat impl(Lucene99CustomCodec.Mode mode) {
        switch (mode) {
            case ZSTD:
                return getCustomCompressingStoredFieldsFormat("CustomStoredFieldsZstd", this.zstdCompressionMode);
            case ZSTD_NO_DICT:
                return getCustomCompressingStoredFieldsFormat("CustomStoredFieldsZstdNoDict", this.zstdNoDictCompressionMode);
            case QAT_DEFLATE:
                return getCustomCompressingStoredFieldsFormat(
                    "CustomStoredFieldsQatDeflate",
                    this.qatDeflateCompressionMode,
                    QAT_DEFLATE_BLOCK_LENGTH,
                    QAT_DEFLATE_MAX_DOCS_PER_BLOCK,
                    QAT_DEFLATE_BLOCK_SHIFT
                );
            case QAT_LZ4:
                return getCustomCompressingStoredFieldsFormat(
                    "CustomStoredFieldsQatLz4",
                    this.qatLz4CompressionMode,
                    QAT_LZ4_BLOCK_LENGTH,
                    QAT_DEFLATE_MAX_DOCS_PER_BLOCK,
                    QAT_DEFLATE_BLOCK_SHIFT
                );
            default:
                throw new AssertionError();
        }
    }

    private StoredFieldsFormat getCustomCompressingStoredFieldsFormat(String formatName, CompressionMode compressionMode) {
        return getCustomCompressingStoredFieldsFormat(
            formatName,
            compressionMode,
            ZSTD_BLOCK_LENGTH,
            ZSTD_MAX_DOCS_PER_BLOCK,
            ZSTD_BLOCK_SHIFT
        );
    }

    private StoredFieldsFormat getCustomCompressingStoredFieldsFormat(
        String formatName,
        CompressionMode compressionMode,
        int blockSize,
        int maxDocs,
        int blockShift
    ) {
        return new Lucene90CompressingStoredFieldsFormat(formatName, compressionMode, blockSize, maxDocs, blockShift);
    }

    public Lucene99CustomCodec.Mode getMode() {
        return mode;
    }

    /** Returns the compression level. */
    public int getCompressionLevel() {
        return compressionLevel;
    }

    public CompressionMode getCompressionMode() {
        switch (mode) {
            case ZSTD:
                return zstdCompressionMode;
            case ZSTD_NO_DICT:
                return zstdNoDictCompressionMode;
            case QAT_DEFLATE:
                return qatDeflateCompressionMode;
            case QAT_LZ4:
                return qatLz4CompressionMode;
            default:
                throw new AssertionError();
        }
    }
}
