/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.codec.CodecAliases;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.MapperService;

import java.util.Set;

/**
 * ZstdNoDictCodec provides ZSTD compressor without a dictionary support.
 */
public class ZstdNoDict99Codec extends Lucene99CustomCodec implements CodecSettings, CodecAliases {

    /**
     * Creates a new ZstdNoDictCodec instance with the default compression level.
     */
    public ZstdNoDict99Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new ZstdNoDictCodec instance.
     *
     * @param compressionLevel The compression level.
     */
    public ZstdNoDict99Codec(int compressionLevel) {
        super(Mode.ZSTD_NO_DICT, compressionLevel);
    }

    /**
     * Creates a new ZstdNoDictCodec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     */
    public ZstdNoDict99Codec(MapperService mapperService, Logger logger, int compressionLevel) {
        super(Mode.ZSTD_NO_DICT, compressionLevel, mapperService, logger);
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
        return Mode.ZSTD_NO_DICT.getAliases();
    }
}
