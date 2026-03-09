/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;

import java.util.Set;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;

/**
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec.
 *
 * @opensearch.internal
 */
public abstract class Lucene104QatCodec extends FilterCodec {
    /** Each mode represents a compression algorithm. */
    public enum Mode {
        /** QAT lz4 mode. */
        QAT_LZ4("QATLZ4104", Set.of("qat_lz4")),

        /** QAT deflate mode. */
        QAT_DEFLATE("QATDEFLATE104", Set.of("qat_deflate")),

        /** QAT zstd mode. */
        QAT_ZSTD("QATZSTD104", Set.of("qat_zstd"));

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
    public Lucene104QatCodec(Mode mode) {
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
    public Lucene104QatCodec(Mode mode, int compressionLevel) {
        super(mode.getCodec(), new Lucene104Codec());
        this.storedFieldsFormat = new Lucene104QatStoredFieldsFormat(mode, compressionLevel);
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
    public Lucene104QatCodec(Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(mode.getCodec(), new Lucene104Codec());
        this.storedFieldsFormat = new Lucene104QatStoredFieldsFormat(mode, compressionLevel, supplier);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     * @param defaultCodecSupplier default opensearch codec supplier
     * @param compressionLevel The compression level.
     */
    public Lucene104QatCodec(Mode mode, Supplier<Codec> defaultCodecSupplier, int compressionLevel) {
        super(mode.getCodec(), defaultCodecSupplier.get());
        this.storedFieldsFormat = new Lucene104QatStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use lowercase letters when
     * registering the codec so that we remain consistent with the other compression codecs: default,
     * lucene_default, and best_compression.
     *
     * @param mode The compression codec (QAT_LZ4, QAT_DEFLATE, or QAT_ZSTD).
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT mode.
     * @param defaultCodecSupplier default opensearch codec supplier
     */
    public Lucene104QatCodec(Mode mode, int compressionLevel, Supplier<QatZipper.Mode> supplier, Supplier<Codec> defaultCodecSupplier) {
        super(mode.getCodec(), defaultCodecSupplier.get());
        this.storedFieldsFormat = new Lucene104QatStoredFieldsFormat(mode, compressionLevel, supplier);
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
