/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.Codec;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.codec.CodecAliases;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.engine.EngineConfig;

import java.util.Set;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;

/**
 * QatZstd101Codec provides a ZSTD compressor using the <a
 * href="https://github.com/intel/qat-java">qat-java</a> library.
 */
public class QatZstd101Codec extends Lucene101QatCodec implements CodecSettings, CodecAliases {

    /** Creates a new QatZstd101Codec instance with the default compression level. */
    public QatZstd101Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new QatZstd101Codec instance.
     *
     * @param compressionLevel The compression level.
     */
    public QatZstd101Codec(int compressionLevel) {
        super(Mode.QAT_ZSTD, compressionLevel);
    }

    /**
     * Creates a new QatZstd101Codec instance with the default compression level.
     *
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     */
    public QatZstd101Codec(int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(Mode.QAT_ZSTD, compressionLevel, supplier);
    }

    /**
     * Creates a new QatZstd101Codec instance.
     *
     * @param defaultCodecSupplier default opensearch codec supplier
     * @param compressionLevel The compression level.
     */
    public QatZstd101Codec(Supplier<Codec> defaultCodecSupplier, int compressionLevel) {
        super(Mode.QAT_ZSTD, defaultCodecSupplier, compressionLevel);
    }

    /**
     * Creates a new QatZstd101Codec instance.
     *
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     * @param defaultCodecSupplier default opensearch codec supplier
     */
    public QatZstd101Codec(int compressionLevel, Supplier<QatZipper.Mode> supplier, Supplier<Codec> defaultCodecSupplier) {
        super(Mode.QAT_ZSTD, compressionLevel, supplier, defaultCodecSupplier);
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
