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
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.mapper.MapperService;

import java.util.Set;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;

/**
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec.
 *
 * @opensearch.internal
 */
public abstract class Lucene912QatCodec extends FilterCodec {
    /** Each mode represents a compression algorithm. */
    public enum Mode {
        /** QAT lz4 mode. */
        QAT_LZ4("QATLZ4912", Set.of("qat_lz4")),

        /** QAT deflate mode. */
        QAT_DEFLATE("QATDEFLATE912", Set.of("qat_deflate")),

        /** QAT zstd mode. */
        QAT_ZSTD("QATZSTD912", Set.of("qat_zstd"));

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

    /** The default compression mode. */
    public static final Mode DEFAULT_COMPRESSION_MODE = Mode.QAT_LZ4;

    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * Creates a new compression codec with the default compression level.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     */
    public Lucene912QatCodec(Mode mode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     * @param compressionLevel The compression level.
     */
    public Lucene912QatCodec(Mode mode, int compressionLevel) {
        super(mode.getCodec(), new Lucene912Codec());
        this.storedFieldsFormat = new Lucene912QatStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT mode.
     */
    public Lucene912QatCodec(Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(mode.getCodec(), new Lucene912Codec());
        this.storedFieldsFormat = new Lucene912QatStoredFieldsFormat(mode, compressionLevel, supplier);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     * @param compressionLevel The compression level.
     * @param mapperService The mapper service.
     * @param logger The logger.
     */
    public Lucene912QatCodec(Mode mode, int compressionLevel, MapperService mapperService, Logger logger) {
        super(mode.getCodec(), new PerFieldMappingPostingFormatCodec(Lucene912Codec.Mode.BEST_SPEED, mapperService, logger));
        this.storedFieldsFormat = new Lucene912QatStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     * @param compressionLevel The compression level.
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param supplier supplier for QAT mode.
     */
    public Lucene912QatCodec(
        Mode mode,
        int compressionLevel,
        MapperService mapperService,
        Logger logger,
        Supplier<QatZipper.Mode> supplier
    ) {
        super(mode.getCodec(), new PerFieldMappingPostingFormatCodec(Lucene912Codec.Mode.BEST_SPEED, mapperService, logger));
        this.storedFieldsFormat = new Lucene912QatStoredFieldsFormat(mode, compressionLevel, supplier);
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
