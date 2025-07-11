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
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import org.mockito.Mockito;

import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_DEFLATE_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_LZ4_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.QAT_ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_CODEC;
import static org.opensearch.index.codec.customcodecs.CustomCodecService.ZSTD_NO_DICT_CODEC;
import static org.opensearch.index.codec.customcodecs.backward_codecs.lucene99.Lucene99CustomCodec.DEFAULT_COMPRESSION_LEVEL;
import static org.opensearch.index.engine.EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CustomCodecTests extends OpenSearchTestCase {

    private CustomCodecPlugin plugin;

    @Before
    public void setup() {
        plugin = new CustomCodecPlugin();
    }

    public void testZstd() throws Exception {
        Codec codec = createCodecService(false).codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdWithCompositeIndex() throws Exception {
        Codec codec = createCodecService(false, true).codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
        // assert docValues to be of compositeCodec's docValuesFormat
        assert codec.docValuesFormat() instanceof Composite912DocValuesFormat;
    }

    public void testZstdNoDict() throws Exception {
        Codec codec = createCodecService(false).codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictWithCompositeIndex() throws Exception {
        Codec codec = createCodecService(false, true).codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
        // assert docValues to be of compositeCodec's docValuesFormat
        assert codec.docValuesFormat() instanceof Composite912DocValuesFormat;
    }

    public void testZstdDeprecatedCodec() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createCodecService(false).codec("ZSTD_DEPRECATED")
        );
        assertTrue(e.getMessage().startsWith("failed to find codec"));
    }

    public void testZstdWithCompressionLevel() throws Exception {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "zstd").codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictWithCompressionLevel() throws Exception {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "zstd_no_dict").codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionLevel());
    }

    public void testBestCompressionWithCompressionLevel() {
        final Settings zstdSettings = Settings.builder()
            .put(INDEX_CODEC_COMPRESSION_LEVEL_SETTING.getKey(), randomIntBetween(1, 6))
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), randomFrom(ZSTD_CODEC, ZSTD_NO_DICT_CODEC))
            .build();

        // able to validate zstd
        final IndexScopedSettings zstdIndexScopedSettings = new IndexScopedSettings(
            zstdSettings,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );
        zstdIndexScopedSettings.validate(zstdSettings, true);
    }

    public void testLuceneCodecsWithCompressionLevel() {
        final Settings customCodecSettings = Settings.builder()
            .put(INDEX_CODEC_COMPRESSION_LEVEL_SETTING.getKey(), randomIntBetween(1, 6))
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), randomFrom("zstd", "zstd_no_dict"))
            .build();

        final IndexScopedSettings customCodecIndexScopedSettings = new IndexScopedSettings(
            customCodecSettings,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );
        customCodecIndexScopedSettings.validate(customCodecSettings, true);
    }

    public void testZstandardCompressionLevelSupport() throws Exception {
        CodecService codecService = createCodecService(false);
        CodecSettings zstdCodec = (CodecSettings) codecService.codec("zstd");
        CodecSettings zstdNoDictCodec = (CodecSettings) codecService.codec("zstd_no_dict");
        assertTrue(zstdCodec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
        assertTrue(zstdNoDictCodec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
    }

    public void testDefaultMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("default");
        assertStoredFieldsCompressionEquals(Lucene101Codec.Mode.BEST_SPEED, codec);
    }

    public void testBestCompressionMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("best_compression");
        assertStoredFieldsCompressionEquals(Lucene101Codec.Mode.BEST_COMPRESSION, codec);
    }

    public void testZstdMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene101CustomStoredFieldsFormat storedFieldsFormat = (Lucene101CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testQatCodecsNotAvailable() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) {
            assertThrows(IllegalArgumentException.class, () -> createCodecService(false).codec("qat_lz4"));
            assertThrows(IllegalArgumentException.class, () -> createCodecService(false).codec("qat_deflate"));
            assertThrows(IllegalArgumentException.class, () -> createCodecService(false).codec("qat_zstd"));

            QatLz4101Codec qatLz4101Codec = new QatLz4101Codec();
            assertTrue(qatLz4101Codec.aliases().isEmpty());

            QatDeflate101Codec qatDeflate101Codec = new QatDeflate101Codec();
            assertTrue(qatDeflate101Codec.aliases().isEmpty());

            QatZstd101Codec qatZstd101Codec = new QatZstd101Codec();
            assertTrue(qatZstd101Codec.aliases().isEmpty());
        }
    }

    public void testCodecServiceFactoryQatUnavailable() throws IOException {
        if (!QatZipperFactory.isQatAvailable()) {
            Settings nodeSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .put("index.codec", randomFrom(QAT_DEFLATE_CODEC, QAT_LZ4_CODEC, QAT_ZSTD_CODEC))
                .build();
            IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
            assertThrows(IllegalArgumentException.class, () -> plugin.getCustomCodecServiceFactory(indexSettings));
        }
    }

    // write some docs with it, inspect .si to see this was the used compression
    private void assertStoredFieldsCompressionEquals(Lucene101Codec.Mode expected, Codec actual) throws Exception {
        SegmentReader sr = getSegmentReader(actual);
        String v = sr.getSegmentInfo().info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene101Codec.Mode.valueOf(v));
    }

    private void assertStoredFieldsCompressionEquals(Lucene101CustomCodec.Mode expected, Codec actual) throws Exception {
        SegmentReader sr = getSegmentReader(actual);
        String v = sr.getSegmentInfo().info.getAttribute(Lucene101CustomStoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene101CustomCodec.Mode.valueOf(v));
    }

    private CodecService createCodecService(boolean isMapperServiceNull) throws IOException {
        return createCodecService(isMapperServiceNull, false);
    }

    private CodecService createCodecService(boolean isMapperServiceNull, boolean isCompositeIndexPresent) throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        if (isMapperServiceNull) {
            return new CustomCodecService(null, IndexSettingsModule.newIndexSettings("_na", nodeSettings), LogManager.getLogger("test"));
        }
        if (isCompositeIndexPresent) {
            return buildCodecServiceWithCompositeIndex(nodeSettings);
        }
        return buildCodecService(nodeSettings);
    }

    private CodecService buildCodecServiceWithCompositeIndex(Settings nodeSettings) throws IOException {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        MapperService service = Mockito.mock(MapperService.class);
        Mockito.when(service.isCompositeIndexPresent()).thenReturn(true);
        return new CustomCodecService(service, indexSettings, LogManager.getLogger("test"));
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

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
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
            return customCodecServiceFactory.get().createCodecService(new CodecServiceConfig(indexSettings, service, logger));
        }
        return new CustomCodecService(service, indexSettings, LogManager.getLogger("test"));
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
