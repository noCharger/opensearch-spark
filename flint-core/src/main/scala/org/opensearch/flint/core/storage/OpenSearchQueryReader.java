/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.Logger;

import static org.opensearch.flint.core.metrics.MetricConstants.REQUEST_METADATA_READ_METRIC_PREFIX;

/**
 * {@link OpenSearchReader} using search. https://opensearch.org/docs/latest/api-reference/search/
 */
public class OpenSearchQueryReader extends OpenSearchReader {

  private static final Logger LOG = Logger.getLogger(OpenSearchQueryReader.class.getName());

  public OpenSearchQueryReader(IRestHighLevelClient client, String indexName, SearchSourceBuilder searchSourceBuilder) {
    super(client, new SearchRequest().indices(indexName).source(searchSourceBuilder));
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) {
    Optional<SearchResponse> response = Optional.empty();

    // Log the raw query
    LOG.info(String.format("Executing search on index [%s] with query: %s",
            String.join(",", request.indices()),
            request.source().toString()));

    long startTime = System.currentTimeMillis();

    try {
      response = Optional.of(client.search(request, RequestOptions.DEFAULT));
      IRestHighLevelClient.recordOperationSuccess(REQUEST_METADATA_READ_METRIC_PREFIX);

      long endTime = System.currentTimeMillis();
      long latency = endTime - startTime;
      LOG.info(String.format("Search completed on index [%s] in %d ms",
              String.join(",", request.indices()), latency));

    } catch (Exception e) {
      IRestHighLevelClient.recordOperationFailure(REQUEST_METADATA_READ_METRIC_PREFIX, e);
      LOG.warning(String.format("Search failed on index [%s]: %s",
              String.join(",", request.indices()), e.getMessage()));
    }
    return response;
  }

  /**
   * nothing to clean
   */
  void clean() throws IOException {}
}