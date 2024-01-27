/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs;

import org.opensearch.common.settings.Setting;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.engine.EngineConfig;

/**
 * ZstdDeprecatedCodec provides ZSTD compressor using the <a href="https://github.com/luben/zstd-jni">zstd-jni</a> library.
 * Added to support backward compatibility for indices created with Lucene95CustomCodec as codec name.
 */
@Deprecated(since = "2.10")
public class Zstd95DeprecatedCodec extends Lucene95CustomCodec implements CodecSettings {

    /**
     * Creates a new ZstdDefaultCodec instance with the default compression level.
     */
    public Zstd95DeprecatedCodec() {
        super(Mode.ZSTD_DEPRECATED);
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
}
