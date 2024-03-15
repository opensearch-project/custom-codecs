/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import com.intel.qat.QatZipper;

import static com.intel.qat.QatZipper.Algorithm;
import static com.intel.qat.QatZipper.DEFAULT_COMPRESS_LEVEL;
import static com.intel.qat.QatZipper.DEFAULT_MODE;
import static com.intel.qat.QatZipper.DEFAULT_POLLING_MODE;
import static com.intel.qat.QatZipper.DEFAULT_RETRY_COUNT;
import static com.intel.qat.QatZipper.Mode;
import static com.intel.qat.QatZipper.PollingMode;

/** A factory class to create instances of QatZipper */
public class QatZipperFactory

{

    /**
     * Creates a new QatZipper with the specified parameters.
     *
     * @param algorithm the compression algorithm
     * @param level the compression level.
     * @param mode the mode of QAT execution
     * @param retryCount the number of attempts to acquire hardware resources
     * @param pmode polling mode.
     */
    public static QatZipper createInstance(Algorithm algorithm, int level, Mode mode, int retryCount, PollingMode pmode) {
        return new QatZipper(algorithm, level, mode, retryCount, pmode);
    }

    /**
     * Creates a new QatZipper that uses the DEFLATE algorithm and the default compression level,
     * mode, retry count, and polling mode.
     */
    public static QatZipper createInstance() {
        return createInstance(Algorithm.DEFLATE, DEFAULT_COMPRESS_LEVEL, DEFAULT_MODE, DEFAULT_RETRY_COUNT, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified compression algorithm. Uses the default compression
     * level, mode, retry count, and polling mode.
     *
     * @param algorithm the compression algorithm
     */
    public static QatZipper createInstance(Algorithm algorithm) {
        return createInstance(algorithm, DEFAULT_COMPRESS_LEVEL, DEFAULT_MODE, DEFAULT_RETRY_COUNT, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified execution mode. Uses the DEFLATE algorithm with the
     * default compression level, retry count, and polling mode.
     *
     * @param mode the mode of QAT execution
     */
    public static QatZipper createInstance(Mode mode) {
        return createInstance(Algorithm.DEFLATE, DEFAULT_COMPRESS_LEVEL, mode, DEFAULT_RETRY_COUNT, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified polling polling mode. Uses the DEFLATE algorithm
     * with the default compression level, mode, and retry count.
     *
     * @param pmode the polling mode.
     */
    public static QatZipper createInstance(PollingMode pmode) {
        return createInstance(Algorithm.DEFLATE, DEFAULT_COMPRESS_LEVEL, DEFAULT_MODE, DEFAULT_RETRY_COUNT, pmode);
    }

    /**
     * Creates a new QatZipper with the specified algorithm and compression level. Uses the default
     * mode, retry count, and polling mode.
     *
     * @param algorithm the compression algorithm (deflate or LZ4).
     * @param level the compression level.
     */
    public static QatZipper createInstance(Algorithm algorithm, int level) {
        return createInstance(algorithm, level, DEFAULT_MODE, DEFAULT_RETRY_COUNT, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified algorithm and mode of execution. Uses the default
     * compression level, retry count, and polling mode.
     *
     * @param algorithm the compression algorithm
     * @param mode the mode of QAT execution
     */
    public static QatZipper createInstance(Algorithm algorithm, Mode mode) {
        return createInstance(algorithm, DEFAULT_COMPRESS_LEVEL, mode, DEFAULT_RETRY_COUNT, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified algorithm and polling mode of execution. Uses the
     * default compression level, mode, and retry count.
     *
     * @param algorithm the compression algorithm
     * @param pmode the polling mode.
     */
    public static QatZipper createInstance(Algorithm algorithm, PollingMode pmode) {
        return createInstance(algorithm, DEFAULT_COMPRESS_LEVEL, DEFAULT_MODE, DEFAULT_RETRY_COUNT, pmode);
    }

    /**
     * Creates a new QatZipper with the specified algorithm and mode of execution. Uses compression
     * level and retry count.
     *
     * @param algorithm the compression algorithm
     * @param mode the mode of QAT execution
     * @param pmode the polling mode.
     */
    public static QatZipper createInstance(Algorithm algorithm, Mode mode, PollingMode pmode) {
        return createInstance(algorithm, DEFAULT_COMPRESS_LEVEL, mode, DEFAULT_RETRY_COUNT, pmode);
    }

    /**
     * Creates a new QatZipper with the specified algorithm, compression level, and mode . Uses the
     * default retry count and polling mode.
     *
     * @param algorithm the compression algorithm (deflate or LZ4).
     * @param level the compression level.
     * @param mode the mode of operation (HARDWARE - only hardware, AUTO - hardware with a software
     *     failover.)
     */
    public static QatZipper createInstance(Algorithm algorithm, int level, Mode mode) {
        return createInstance(algorithm, level, mode, DEFAULT_RETRY_COUNT, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified algorithm, compression level, and polling mode .
     * Uses the default mode and retry count.
     *
     * @param algorithm the compression algorithm (deflate or LZ4).
     * @param level the compression level.
     * @param pmode the polling mode.
     */
    public static QatZipper createInstance(Algorithm algorithm, int level, PollingMode pmode) {
        return createInstance(algorithm, level, DEFAULT_MODE, DEFAULT_RETRY_COUNT, pmode);
    }

    /**
     * Creates a new QatZipper with the specified parameters and polling mode.
     *
     * @param algorithm the compression algorithm
     * @param level the compression level.
     * @param mode the mode of QAT execution
     * @param retryCount the number of attempts to acquire hardware resources
     */
    public static QatZipper createInstance(Algorithm algorithm, int level, Mode mode, int retryCount) {
        return createInstance(algorithm, level, mode, retryCount, DEFAULT_POLLING_MODE);
    }

    /**
     * Creates a new QatZipper with the specified parameters and retry count.
     *
     * @param algorithm the compression algorithm
     * @param level the compression level.
     * @param mode the mode of QAT execution
     * @param pmode the polling mode.
     */
    public static QatZipper createInstance(Algorithm algorithm, int level, Mode mode, PollingMode pmode) {
        return createInstance(algorithm, level, mode, DEFAULT_RETRY_COUNT, pmode);
    }

    /**
     * Checks if QAT hardware is available.
     *
     * @return true if QAT hardware is available, false otherwise.
     */
    public static boolean isQatAvailable() {
        try {
            QatZipper qzip = QatZipperFactory.createInstance();
            qzip.end();
            return true;
        } catch (UnsatisfiedLinkError | ExceptionInInitializerError | NoClassDefFoundError e) {
            return false;
        }
    }
}
