/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs.lucene103;

import org.apache.lucene.backward_codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;

import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99CustomCodec.DEFAULT_COMPRESSION_LEVEL;

/**
 *
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec.
 * Supports two modes zstd and zstd_no_dict.
 * Uses Lucene103 as the delegate codec
 *
 * @opensearch.internal
 */
public abstract class Lucene103CustomCodec extends FilterCodec {

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        /**
         * ZStandard mode with dictionary
         */
        ZSTD("ZSTD103", Set.of("zstd")),
        /**
         * ZStandard mode without dictionary
         */
        ZSTD_NO_DICT("ZSTDNODICT103", Set.of("zstd_no_dict"));

        private final String codec;
        private final Set<String> aliases;

        Mode(String codec, Set<String> aliases) {
            this.codec = codec;
            this.aliases = aliases;
        }

        /**
         * Returns the Codec that is registered with Lucene
         */
        public String getCodec() {
            return codec;
        }

        /**
         * Returns the aliases of the Codec
         */
        public Set<String> getAliases() {
            return aliases;
        }
    }

    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * Creates a new compression codec with the default compression level.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     */
    public Lucene103CustomCodec(Mode mode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new compression codec with the given compression level. We use
     * lowercase letters when registering the codec so that we remain consistent with
     * the other compression codecs: default, lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     * @param compressionLevel The compression level.
     */
    public Lucene103CustomCodec(Mode mode, int compressionLevel) {
        super(mode.getCodec(), new Lucene103Codec());
        this.storedFieldsFormat = new Lucene103CustomStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use
     * lowercase letters when registering the codec so that we remain consistent with
     * the other compression codecs: default, lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     * @param compressionLevel The compression level.
     * @param defaultCodecSupplier Default OpenSearch codec supplier
     */
    public Lucene103CustomCodec(Mode mode, int compressionLevel, Supplier<Codec> defaultCodecSupplier) {
        super(mode.getCodec(), defaultCodecSupplier.get());
        this.storedFieldsFormat = new Lucene103CustomStoredFieldsFormat(mode, compressionLevel);
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
