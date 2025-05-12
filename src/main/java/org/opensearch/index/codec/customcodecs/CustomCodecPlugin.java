/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.common.settings.Setting;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * A plugin that implements custom codecs. Supports these codecs:
 *
 * <ul>
 *   <li>ZSTD_CODEC
 *   <li>ZSTD_NO_DICT_CODEC
 *   <li>QAT_LZ4
 *   <li>QAT_DEFLATE
 *   <li>QAT_ZSTD
 * </ul>
 *
 * @opensearch.internal
 */
public final class CustomCodecPlugin extends Plugin implements EnginePlugin {

    /** Creates a new instance */
    public CustomCodecPlugin() {}

    /**
     * @param indexSettings is the default indexSettings
     * @return the engine factory
     */
    @Override
    public Optional<CodecServiceFactory> getCustomCodecServiceFactory(final IndexSettings indexSettings) {
        String codecName = indexSettings.getValue(EngineConfig.INDEX_CODEC_SETTING);
        if (codecName.equals(CustomCodecService.ZSTD_NO_DICT_CODEC)
            || codecName.equals(CustomCodecService.ZSTD_CODEC)
            || codecName.equals(CustomCodecService.QAT_LZ4_CODEC)
            || codecName.equals(CustomCodecService.QAT_DEFLATE_CODEC)
            || codecName.equals(CustomCodecService.QAT_ZSTD_CODEC)) {
            return Optional.of(new CustomCodecServiceFactory());
        } else {
            if (!QatZipperFactory.isQatAvailable() && isQatCodec(codecName)) {
                throw new IllegalArgumentException(
                    "QAT codecs are not supported (QAT is not available). Please create indices with a different codec."
                );
            }
        }
        return Optional.empty();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(Lucene99QatCodec.INDEX_CODEC_QAT_MODE_SETTING);
    }

    private static boolean isQatCodec(String codecName) {
        return codecName.equals(Lucene99QatCodec.Mode.QAT_LZ4.getCodec())
            || codecName.equals(Lucene99QatCodec.Mode.QAT_DEFLATE.getCodec())
            || codecName.equals(Lucene912QatCodec.Mode.QAT_LZ4.getCodec())
            || codecName.equals(Lucene912QatCodec.Mode.QAT_DEFLATE.getCodec());
    }
}
