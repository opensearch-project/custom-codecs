/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs.backward_codecs.lucene103;

import org.apache.lucene.codecs.Codec;
import org.opensearch.common.settings.Setting;
import org.opensearch.index.codec.CodecAliases;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.codec.customcodecs.QatZipperFactory;
import org.opensearch.index.engine.EngineConfig;

import java.util.Set;
import java.util.function.Supplier;

import com.intel.qat.QatZipper;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;

/**
 * QatZstd103Codec provides a ZSTD compressor using the <a
 * href="https://github.com/intel/qat-java">qat-java</a> library.
 */
public class QatZstd103Codec extends Lucene103QatCodec implements CodecSettings, CodecAliases {

    /** Creates a new QatZstd103Codec instance with the default compression level. */
    public QatZstd103Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new QatZstd103Codec instance.
     *
     * @param compressionLevel The compression level.
     */
    public QatZstd103Codec(int compressionLevel) {
        super(Mode.QAT_ZSTD, compressionLevel);
    }

    /**
     * Creates a new QatZstd103Codec instance with the default compression level.
     *
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     */
    public QatZstd103Codec(int compressionLevel, Supplier<QatZipper.Mode> supplier) {
        super(Mode.QAT_ZSTD, compressionLevel, supplier);
    }

    /**
     * Creates a new QatZstd103Codec instance.
     *
     * @param defaultCodecSupplier default opensearch codec supplier
     * @param compressionLevel The compression level.
     */
    public QatZstd103Codec(Supplier<Codec> defaultCodecSupplier, int compressionLevel) {
        super(Mode.QAT_ZSTD, defaultCodecSupplier, compressionLevel);
    }

    /**
     * Creates a new QatZstd103Codec instance.
     *
     * @param compressionLevel The compression level.
     * @param supplier supplier for QAT acceleration mode.
     * @param defaultCodecSupplier default opensearch codec supplier
     */
    public QatZstd103Codec(int compressionLevel, Supplier<QatZipper.Mode> supplier, Supplier<Codec> defaultCodecSupplier) {
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
