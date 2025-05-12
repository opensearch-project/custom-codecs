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

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;

/**
 * QatZstd912Codec provides a ZSTD compressor using the <a
 * href="https://github.com/intel/qat-java">qat-java</a> library.
 */
public class QatZstd912Codec extends Lucene912QatCodec implements CodecSettings, CodecAliases {

    /** Creates a new QatZstd912Codec instance with the default compression level. */
    public QatZstd912Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new QatZstd912Codec instance.
     *
     * @param compressionLevel The compression level.
     */
    public QatZstd912Codec(int compressionLevel) {
        super(Mode.QAT_ZSTD, compressionLevel);
    }

    /**
     * Creates a new QatZstd912Codec instance with the default compression level.
     *
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     */
    public QatZstd912Codec(int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(Mode.QAT_ZSTD, compressionLevel, supplier);
    }

    /**
     * Creates a new QatZstd912Codec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     */
    public QatZstd912Codec(MapperService mapperService, Logger logger, int compressionLevel) {
        super(Mode.QAT_ZSTD, compressionLevel, mapperService, logger);
    }

    /**
     * Creates a new QatZstd912Codec instance.
     *
     * @param mapperService The mapper service.
     * @param logger The logger.
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     */
    public QatZstd912Codec(MapperService mapperService, Logger logger, int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(Mode.QAT_ZSTD, compressionLevel, mapperService, logger, supplier);
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
        if (!QatZipperFactory.isQatAvailable()) {
            return Set.of();
        }
        return Mode.QAT_ZSTD.getAliases();
    }
}
