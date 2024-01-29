/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.metrics;

/**
 * This class defines custom metric constants used for monitoring flint operations.
 */
public class MetricConstants {

    /**
     * The prefix for all read-related metrics in OpenSearch.
     * This constant is used as a part of metric names to categorize and identify metrics related to read operations.
     */
    public static final String OS_READ_OP_METRIC_PREFIX = "opensearch.read";

    /**
     * The prefix for all write-related metrics in OpenSearch.
     * Similar to OS_READ_METRIC_PREFIX, this constant is used for categorizing and identifying metrics that pertain to write operations.
     */
    public static final String OS_WRITE_OP_METRIC_PREFIX = "opensearch.write";
    public static final String REPL_RUNNING_METRIC = "session.running.count";
    public static final String REPL_FAILED_METRIC = "session.failed.count";
    public static final String REPL_SUCCESS_METRIC = "session.success.count";
    public static final String STATEMENT_RUNNING_METRIC = "statement.running.count";
    public static final String STATEMENT_FAILED_METRIC = "statement.failed.count";
    public static final String STATEMENT_SUCCESS_METRIC = "statement.success.count";
}