/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs.lucene99;

import org.opensearch.common.settings.Setting;
import org.opensearch.index.codec.CodecAliases;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.engine.EngineConfig;

import java.util.Set;

/**
 * ZstdCodec provides ZSTD compressor using the <a
 * href="https://github.com/luben/zstd-jni">zstd-jni</a> library.
 */
public class Zstd99Codec extends Lucene99CustomCodec implements CodecSettings, CodecAliases {

    /** Creates a new ZstdCodec instance with the default compression level. */
    public Zstd99Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new ZstdCodec instance.
     *
     * @param compressionLevel The compression level.
     */
    public Zstd99Codec(int compressionLevel) {
        super(Mode.ZSTD, compressionLevel);
    }

    /** The name for this codec. */
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean supports(Setting<?> setting) {
        return setting.equals(EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING);
    }

    @Override
    public Set<String> aliases() {
        return Mode.ZSTD.getAliases();
    }
}
