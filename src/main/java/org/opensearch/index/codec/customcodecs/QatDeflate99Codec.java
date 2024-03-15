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
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

/**
 * QatDeflate99Codec provides a DEFLATE compressor using the <a
 * href="https://github.com/intel/qat-java">qat-java</a> library.
 */
public class QatDeflate99Codec extends Lucene99CustomCodec implements CodecSettings, CodecAliases {

    /** Creates a new QatDeflate99Codec instance with the default compression level. */
    public QatDeflate99Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new QatDeflate99Codec instance.
     *
     * @param compressionLevel The compression level.
     */
    public QatDeflate99Codec(int compressionLevel) {
        super(Mode.QAT_DEFLATE, compressionLevel);
    }

    /**
     * Creates a new QatDeflate99Codec instance with the default compression level.
     *
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     */
    public QatDeflate99Codec(int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(Mode.QAT_DEFLATE, compressionLevel, supplier);
    }

    /**
     * Creates a new QatDeflate99Codec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     */
    public QatDeflate99Codec(MapperService mapperService, Logger logger, int compressionLevel) {
        super(Mode.QAT_DEFLATE, compressionLevel, mapperService, logger);
    }

    /**
     * Creates a new QatDeflate99Codec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     */
    public QatDeflate99Codec(MapperService mapperService, Logger logger, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(Mode.QAT_DEFLATE, compressionLevel, mapperService, logger, supplier);
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
        return Mode.QAT_DEFLATE.getAliases();
    }
}
