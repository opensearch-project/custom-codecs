/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.intel.qat.QatZipper;

/**
 * A plugin that implements custom codecs. Supports these codecs:
 *
 * <ul>
 *   <li>ZSTD_CODEC
 *   <li>ZSTD_NO_DICT_CODEC
 *   <li>QAT_DEFLATE
 *   <li>QAT_LZ4
 * </ul>
 *
 * @opensearch.internal
 */
public final class CustomCodecPlugin extends Plugin implements EnginePlugin {

    /** A setting to specifiy the QAT acceleration mode. */
    public static final Setting<QatZipper.Mode> INDEX_CODEC_QAT_MODE_SETTING = new Setting<>("index.codec.qatmode", "hardware", s -> {
        switch (s) {
            case "auto":
                return QatZipper.Mode.AUTO;
            case "hardware":
                return QatZipper.Mode.HARDWARE;
            default:
                throw new IllegalArgumentException("Unknown value for [index.codec.qatmode] must be one of [auto, hardware] but was: " + s);
        }
    }, Property.IndexScope, Property.Dynamic);

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
            || codecName.equals(CustomCodecService.QAT_DEFLATE_CODEC)
            || codecName.equals(CustomCodecService.QAT_LZ4_CODEC)) {
            return Optional.of(new CustomCodecServiceFactory());
        }
        return Optional.empty();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(INDEX_CODEC_QAT_MODE_SETTING);
    }
}
