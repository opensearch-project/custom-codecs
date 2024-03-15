/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.MapperService;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import static org.opensearch.index.engine.EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING;

/** CustomCodecService provides ZSTD, ZSTD_NO_DICT, QAT_DEFLATE, and QAT_LZ4 compression codecs. */
public class CustomCodecService extends CodecService {
    private final Map<String, Codec> codecs;

    /** ZStandard codec */
    public static final String ZSTD_CODEC = "zstd";

    /** ZStandard without dictionary codec */
    public static final String ZSTD_NO_DICT_CODEC = "zstd_no_dict";

    /** Hardware accelerated (Intel QAT) compression codec for DEFLATE. */
    public static final String QAT_DEFLATE_CODEC = "qat_deflate";

    /** Hardware accelerated (Intel QAT) compression codec for LZ4. */
    public static final String QAT_LZ4_CODEC = "qat_lz4";

    /**
     * Creates a new CustomCodecService.
     *
     * @param mapperService The mapper service.
     * @param indexSettings The index settings.
     * @param logger The logger.
     */
    public CustomCodecService(MapperService mapperService, IndexSettings indexSettings, Logger logger) {
        super(mapperService, indexSettings, logger);
        int compressionLevel = indexSettings.getValue(INDEX_CODEC_COMPRESSION_LEVEL_SETTING);
        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        if (mapperService == null) {
            codecs.put(ZSTD_CODEC, new Zstd99Codec(compressionLevel));
            codecs.put(ZSTD_NO_DICT_CODEC, new ZstdNoDict99Codec(compressionLevel));
            codecs.put(QAT_DEFLATE_CODEC, new QatDeflate99Codec(compressionLevel, () -> {
                return indexSettings.getValue(CustomCodecPlugin.INDEX_CODEC_QAT_MODE_SETTING);
            }));
            codecs.put(QAT_LZ4_CODEC, new QatLz499Codec(compressionLevel, () -> {
                return indexSettings.getValue(CustomCodecPlugin.INDEX_CODEC_QAT_MODE_SETTING);
            }));
        } else {
            codecs.put(ZSTD_CODEC, new Zstd99Codec(mapperService, logger, compressionLevel));
            codecs.put(ZSTD_NO_DICT_CODEC, new ZstdNoDict99Codec(mapperService, logger, compressionLevel));
            codecs.put(QAT_DEFLATE_CODEC, new QatDeflate99Codec(mapperService, logger, compressionLevel, () -> {
                return indexSettings.getValue(CustomCodecPlugin.INDEX_CODEC_QAT_MODE_SETTING);
            }));
            codecs.put(QAT_LZ4_CODEC, new QatLz499Codec(mapperService, logger, compressionLevel, () -> {
                return indexSettings.getValue(CustomCodecPlugin.INDEX_CODEC_QAT_MODE_SETTING);
            }));
        }
        this.codecs = codecs.immutableMap();
    }

    @Override
    public Codec codec(String name) {
        Codec codec = codecs.get(name);
        if (codec == null) {
            return super.codec(name);
        }
        return codec;
    }

    @Override
    public String[] availableCodecs() {
        return Stream.concat(Arrays.stream(super.availableCodecs()), codecs.keySet().stream()).toArray(String[]::new);
    }
}
