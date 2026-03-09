/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.Codec;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.AdditionalCodecs;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.INDEX_CODEC_QAT_MODE_SETTING;
import static org.opensearch.index.engine.EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING;

/** CustomAdditionalCodecs provides ZSTD, ZSTD_NO_DICT, QAT_LZ4, and QAT_DEFLATE compression codecs. */
public class CustomAdditionalCodecs implements AdditionalCodecs {
    /** ZStandard codec */
    public static final String ZSTD_CODEC = "zstd";

    /** ZStandard without dictionary codec */
    public static final String ZSTD_NO_DICT_CODEC = "zstd_no_dict";

    /** Hardware accelerated (Intel QAT) compression codec for LZ4. */
    public static final String QAT_LZ4_CODEC = "qat_lz4";

    /** Hardware accelerated (Intel QAT) compression codec for DEFLATE. */
    public static final String QAT_DEFLATE_CODEC = "qat_deflate";

    /** Hardware accelerated (Intel QAT) compression codec for ZSTD. */
    public static final String QAT_ZSTD_CODEC = "qat_zstd";

    @Override
    public Map<String, Codec> getCodecs(MapperService mapperService, IndexSettings indexSettings, Supplier<Codec> defaultCodec) {
        final int compressionLevel = indexSettings.getValue(INDEX_CODEC_COMPRESSION_LEVEL_SETTING);
        final MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        if (mapperService == null) {
            codecs.put(ZSTD_CODEC, new Zstd104Codec(compressionLevel));
            codecs.put(ZSTD_NO_DICT_CODEC, new ZstdNoDict104Codec(compressionLevel));
            if (QatZipperFactory.isQatAvailable()) {
                codecs.put(
                    QAT_LZ4_CODEC,
                    new QatLz4104Codec(compressionLevel, () -> { return indexSettings.getValue(INDEX_CODEC_QAT_MODE_SETTING); })
                );
                codecs.put(QAT_DEFLATE_CODEC, new QatDeflate104Codec(compressionLevel, () -> {
                    return indexSettings.getValue(INDEX_CODEC_QAT_MODE_SETTING);
                }));
                codecs.put(
                    QAT_ZSTD_CODEC,
                    new QatZstd104Codec(compressionLevel, () -> { return indexSettings.getValue(INDEX_CODEC_QAT_MODE_SETTING); })
                );
            }
        } else {
            codecs.put(ZSTD_CODEC, new Zstd104Codec(compressionLevel, defaultCodec));
            codecs.put(ZSTD_NO_DICT_CODEC, new ZstdNoDict104Codec(compressionLevel, defaultCodec));
            if (QatZipperFactory.isQatAvailable()) {
                codecs.put(
                    QAT_LZ4_CODEC,
                    new QatLz4104Codec(
                        compressionLevel,
                        () -> { return indexSettings.getValue(INDEX_CODEC_QAT_MODE_SETTING); },
                        defaultCodec
                    )
                );
                codecs.put(QAT_DEFLATE_CODEC, new QatDeflate104Codec(compressionLevel, () -> {
                    return indexSettings.getValue(INDEX_CODEC_QAT_MODE_SETTING);
                }, defaultCodec));
                codecs.put(
                    QAT_ZSTD_CODEC,
                    new QatZstd104Codec(
                        compressionLevel,
                        () -> { return indexSettings.getValue(INDEX_CODEC_QAT_MODE_SETTING); },
                        defaultCodec
                    )
                );
            }
        }

        return codecs.immutableMap();
    }
}
