/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs;

import org.apache.lucene.backward_codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;

import java.util.Collections;
import java.util.Set;

/**
 *
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec.
 * Supports two modes zstd and zstd_no_dict.
 * Uses Lucene95 as the delegate codec
 *
 * @opensearch.internal
 */
public abstract class Lucene95CustomCodec extends FilterCodec {

    /** Default compression level used for compression */
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        /**
         * ZStandard mode with dictionary
         */
        ZSTD("ZSTD", Set.of("zstd")),
        /**
         * ZStandard mode without dictionary
         */
        ZSTD_NO_DICT("ZSTDNODICT", Set.of("zstd_no_dict")),
        /**
         * Deprecated ZStandard mode, added for backward compatibility to support indices created in 2.9.0 where
         * both ZSTD and ZSTD_NO_DICT used Lucene95CustomCodec underneath. This should not be used to
         * create new indices.
         */
        ZSTD_DEPRECATED("Lucene95CustomCodec", Collections.emptySet());

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
     * Creates a new compression codec.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     */

    public Lucene95CustomCodec(Mode mode) {
        super(mode.getCodec(), new Lucene95Codec());
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat();
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
