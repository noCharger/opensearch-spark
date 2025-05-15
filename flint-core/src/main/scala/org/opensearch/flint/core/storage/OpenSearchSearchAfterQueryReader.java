/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.common.Strings;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Read OpenSearch Index using PIT with search_after.
 */
public class OpenSearchSearchAfterQueryReader extends OpenSearchReader {

  private static final Logger LOG =
          Logger.getLogger(OpenSearchSearchAfterQueryReader.class.getName());

  /**
   * current search_after value, init value is null
   */
  private Object[] search_after = null;
  public OpenSearchSearchAfterQueryReader(IRestHighLevelClient client, SearchRequest request) {
    super(client, request);
  }

  /**
   * search.
   */
  Optional<SearchResponse> search(SearchRequest request) {
    try {
      // Log initial query details
      LOG.info(String.format("Executing search_after query on index [%s] with query: %s",
              String.join(",", request.indices()),
              request.source().toString()));

      if (search_after != null) {
        LOG.info(String.format("Using search_after values: [%s]",
                Arrays.stream(search_after)
                        .map(Object::toString)
                        .collect(Collectors.joining(","))));
        request.source().searchAfter(search_after);
      }

      long startTime = System.currentTimeMillis();

      Optional<SearchResponse> response = Optional.of(client.search(request, RequestOptions.DEFAULT));

      long endTime = System.currentTimeMillis();
      long latency = endTime - startTime;

      int length = response.get().getHits().getHits().length;
      LOG.info(String.format("Search completed on index [%s] in %d ms, returned %d hits",
              String.join(",", request.indices()), latency, length));

      if (length == 0) {
        LOG.info("No more results found, resetting search_after");
        search_after = null;
        return Optional.empty();
      }

      // update search_after
      search_after = response.get().getHits().getAt(length - 1).getSortValues();
      LOG.info(String.format("Updated search_after to: [%s]",
              Arrays.stream(search_after)
                      .map(Object::toString)
                      .collect(Collectors.joining(","))));

      return response;
    } catch (Exception e) {
      LOG.warning(String.format("Search failed on index [%s]: %s",
              String.join(",", request.indices()),
              e.getMessage()));
      throw new RuntimeException(e);
    }
  }

  /**
   * nothing to clean
   */
  void clean() {}

  static private Function<SearchRequest, SearchRequest> applyPreference(String preference) {
    if (Strings.isNullOrEmpty(preference)) {
      return searchRequest -> searchRequest;
    } else {
      return searchRequest -> searchRequest.preference(preference);
    }
  }
}