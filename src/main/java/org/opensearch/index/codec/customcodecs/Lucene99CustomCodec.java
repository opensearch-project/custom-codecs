/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.mapper.MapperService;

import java.util.Set;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

/**
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec. Supports two modes zstd
 * and zstd_no_dict. Uses Lucene99 as the delegate codec
 *
 * @opensearch.internal
 */
public abstract class Lucene99CustomCodec extends FilterCodec {

    /** Default compression level used for compression */
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    /** The default QAT mode */
    public static final QatZipper.Mode DEFAULT_QAT_MODE = QatZipper.Mode.HARDWARE;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        /** ZStandard mode with dictionary */
        ZSTD("ZSTD99", Set.of("zstd")),
        /** ZStandard mode without dictionary */
        ZSTD_NO_DICT("ZSTDNODICT99", Set.of("zstd_no_dict")),
        /** QAT deflate mode. */
        QAT_DEFLATE("QATDEFLATE99", Set.of("qat_deflate")),
        /** QAT lz4 mode. */
        QAT_LZ4("QATLZ499", Set.of("qat_lz4"));

        private final String codec;
        private final Set<String> aliases;

        Mode(String codec, Set<String> aliases) {
            this.codec = codec;
            this.aliases = aliases;
        }

        /** Returns the Codec that is registered with Lucene */
        public String getCodec() {
            return codec;
        }

        /** Returns the aliases of the Codec */
        public Set<String> getAliases() {
            return aliases;
        }
    }

    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * Creates a new compression codec with the default compression level.
     *
     * @param mode The compression codec (ZSTD, ZSTD_NO_DICT, QAT_DEFLATE, or QAT_LZ4).
     */
    public Lucene99CustomCodec(Mode mode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD, ZSTD_NO_DICT, QAT_DEFLATE, or QAT_LZ4).
     * @param compressionLevel The compression level.
     */
    public Lucene99CustomCodec(Mode mode, int compressionLevel) {
        super(mode.getCodec(), new Lucene99Codec());
        this.storedFieldsFormat = new Lucene99CustomStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD, ZSTD_NO_DICT, QAT_DEFLATE, or QAT_LZ4).
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT mode.
     */
    public Lucene99CustomCodec(Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(mode.getCodec(), new Lucene99Codec());
        this.storedFieldsFormat = new Lucene99CustomStoredFieldsFormat(mode, compressionLevel, supplier);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD, ZSTD_NO_DICT, QAT_DEFLATE, or QAT_LZ4).
     * @param compressionLevel The compression level.
     * @param mapperService The mapper service.
     * @param logger The logger.
     */
    public Lucene99CustomCodec(Mode mode, int compressionLevel, MapperService mapperService, Logger logger) {
        super(mode.getCodec(), new PerFieldMappingPostingFormatCodec(Lucene99Codec.Mode.BEST_SPEED, mapperService, logger));
        this.storedFieldsFormat = new Lucene99CustomStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD, ZSTD_NO_DICT, QAT_DEFLATE, or QAT_LZ4).
     * @param compressionLevel The compression level.
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param supplier supplier for QAT mode.
     */
    public Lucene99CustomCodec(
        Mode mode,
        int compressionLevel,
        MapperService mapperService,
        Logger logger,
        Supplier<QatZipper.Mode> supplier
    ) {
        super(mode.getCodec(), new PerFieldMappingPostingFormatCodec(Lucene99Codec.Mode.BEST_SPEED, mapperService, logger));
        this.storedFieldsFormat = new Lucene99CustomStoredFieldsFormat(mode, compressionLevel, supplier);
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
