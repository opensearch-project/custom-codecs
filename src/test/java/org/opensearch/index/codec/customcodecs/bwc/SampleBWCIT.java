package org.opensearch.index.codec.customcodecs.bwc;

import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.util.ArrayList;
import java.util.List;

public class SampleBWCIT extends OpenSearchRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
//    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");
//    private static final String MIXED_CLUSTER_TEST_ROUND = System.getProperty("tests.rest.bwcsuite_round");
    private String dataIndexName = "test_data_for_ad_plugin";
    private int detectionIntervalInMinutes = 1;
    private int windowDelayIntervalInMinutes = 1;
    private String aggregationMethod = "sum";
    private int totalDocsPerCategory = 10_000;
    private int categoryFieldSize = 2;
    private List<String> runningRealtimeDetectors;
    private List<String> historicalDetectors;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.runningRealtimeDetectors = new ArrayList<>();
        this.historicalDetectors = new ArrayList<>();
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings
                .builder()
                .put(super.restClientSettings())
                // increase the timeout here to 90 seconds to handle long waits for a green
                // cluster health. the waits for green need to be longer than a minute to
                // account for delayed shards
                .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
                .build();
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }


    @SuppressWarnings("unchecked")
    public void testBackwardsCompatibility() throws Exception {
        logger.info("Cluster type ::: -> " + CLUSTER_TYPE);
    }
}
