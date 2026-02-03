/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL;
import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99QatCodec.INDEX_CODEC_QAT_MODE_SETTING;
import static org.opensearch.index.engine.EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeThat;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class QatCodecTests extends OpenSearchTestCase {

    private CustomCodecPlugin plugin;

    @Before
    public void setup() {
        plugin = new CustomCodecPlugin();
    }

    public void testQatLz4() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Codec codec = createCodecService(false).codec("qat_lz4");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_LZ4, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatDeflate() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Codec codec = createCodecService(false).codec("qat_deflate");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_DEFLATE, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatZstd() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Codec codec = createCodecService(false).codec("qat_zstd");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_ZSTD, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatLz4WithCompressionLevel() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "qat_lz4").codec("qat_lz4");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_LZ4, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatDeflateWithCompressionLevel() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "qat_deflate").codec("qat_deflate");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_DEFLATE, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatZstdWithCompressionLevel() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "qat_zstd").codec("qat_zstd");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_ZSTD, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatCompressionLevelSupport() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        CodecService codecService = createCodecService(false);
        CodecSettings qatDeflateCodec = (CodecSettings) codecService.codec("qat_deflate");
        CodecSettings qatLz4Codec = (CodecSettings) codecService.codec("qat_lz4");
        CodecSettings qatZstdCodec = (CodecSettings) codecService.codec("qat_zstd");
        assertTrue(qatDeflateCodec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
        assertTrue(qatLz4Codec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
        assertTrue(qatZstdCodec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
    }

    public void testQatLz4MapperServiceNull() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Codec codec = createCodecService(true).codec("qat_lz4");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_LZ4, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatDeflateMapperServiceNull() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Codec codec = createCodecService(true).codec("qat_deflate");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_DEFLATE, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    public void testQatZstdMapperServiceNull() throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        Codec codec = createCodecService(true).codec("qat_zstd");
        assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode.QAT_ZSTD, codec);
        Lucene103QatStoredFieldsFormat storedFieldsFormat = (Lucene103QatStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionMode().getCompressionLevel());
    }

    private void assertStoredFieldsCompressionEquals(Lucene103QatCodec.Mode expected, Codec actual) throws Exception {
        assumeThat("Qat library is available", QatZipperFactory.isQatAvailable(), is(true));
        SegmentReader sr = getSegmentReader(actual);
        String v = sr.getSegmentInfo().info.getAttribute(Lucene103QatStoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene103QatCodec.Mode.valueOf(v));
    }

    private CodecService createCodecService(boolean isMapperServiceNull) throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        if (isMapperServiceNull) {
            return new CustomCodecService(
                null,
                IndexSettingsModule.newIndexSettings("_na", nodeSettings, INDEX_CODEC_QAT_MODE_SETTING),
                LogManager.getLogger("test"),
                List.of()
            );
        }
        return buildCodecService(nodeSettings);
    }

    private CodecService createCodecService(int randomCompressionLevel, String codec) throws IOException {
        Settings nodeSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("index.codec", codec)
            .put("index.codec.compression_level", randomCompressionLevel)
            .build();
        return buildCodecService(nodeSettings);
    }

    private CodecService buildCodecService(Settings nodeSettings) throws IOException {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings, INDEX_CODEC_QAT_MODE_SETTING);
        SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
        IndexAnalyzers indexAnalyzers = createTestAnalysis(indexSettings, nodeSettings).indexAnalyzers;
        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
        MapperService service = new MapperService(
            indexSettings,
            indexAnalyzers,
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> null,
            () -> false,
            null
        );

        Optional<CodecServiceFactory> customCodecServiceFactory = plugin.getCustomCodecServiceFactory(indexSettings);
        if (customCodecServiceFactory.isPresent()) {
            return customCodecServiceFactory.get().createCodecService(new CodecServiceConfig(indexSettings, service, logger, List.of()));
        }
        return new CustomCodecService(service, indexSettings, LogManager.getLogger("test"), List.of());
    }

    private SegmentReader getSegmentReader(Codec codec) throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setCodec(codec);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.addDocument(new Document());
        iw.commit();
        iw.close();
        DirectoryReader ir = DirectoryReader.open(dir);
        SegmentReader sr = (SegmentReader) ir.leaves().get(0).reader();
        ir.close();
        dir.close();
        return sr;
    }
}
