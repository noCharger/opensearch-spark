/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table;

import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.flint.core.FlintOptions;
import org.opensearch.flint.core.IRestHighLevelClient;
import org.opensearch.flint.core.MetaData;
import org.opensearch.flint.core.storage.OpenSearchClientUtils;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class OpenSearchCluster {

  private static final Logger LOG = Logger.getLogger(OpenSearchCluster.class.getName());

  /**
   * Creates list of OpenSearchIndexTable instance of indices in OpenSearch domain.
   *
   * @param indexName
   *   tableName support (1) single index name. (2) wildcard index name. (3) comma sep index name.
   * @param options
   *   The options for Flint.
   * @return
   *   A list of OpenSearchIndexTable instance.
   */
  public static List<OpenSearchIndexTable> apply(String indexName, FlintOptions options) {
    return getAllOpenSearchTableMetadata(options, indexName.split(","))
        .stream()
        .map(metadata -> new OpenSearchIndexTable(metadata, options))
        .collect(Collectors.toList());
  }

  /**
   * Retrieve all metadata for OpenSearch table whose name matches the given pattern.
   *
   * @param options The options for Flint.
   * @param indexNamePattern index name pattern
   * @return list of OpenSearch table metadata
   */
  public static List<MetaData> getAllOpenSearchTableMetadata(FlintOptions options, String... indexNamePattern) {
    long startTime = System.currentTimeMillis();
    LOG.info("Fetching all OpenSearch table metadata for pattern " + String.join(",", indexNamePattern));

    String[] indexNames =
            Arrays.stream(indexNamePattern).map(OpenSearchClientUtils::sanitizeIndexName).toArray(String[]::new);

    try (IRestHighLevelClient client = OpenSearchClientUtils.createClient(options)) {
      GetIndexRequest request = new GetIndexRequest(indexNames);

      long queryStartTime = System.currentTimeMillis();
      GetIndexResponse response = client.getIndex(request, RequestOptions.DEFAULT);
      long queryEndTime = System.currentTimeMillis();

      List<MetaData> result = Arrays.stream(response.getIndices())
              .map(index -> new MetaData(
                      index,
                      response.getMappings().get(index).source().string(),
                      response.getSettings().get(index).toString()))
              .collect(Collectors.toList());

      long endTime = System.currentTimeMillis();
      LOG.info(String.format("Fetched metadata for %d indices matching [%s] in %.3f seconds " +
                      "(OpenSearch query: %.3f seconds, Processing: %.3f seconds)",
              result.size(),
              String.join(",", indexNamePattern),
              (endTime - startTime) / 1000.0,
              (queryEndTime - queryStartTime) / 1000.0,
              (endTime - queryEndTime) / 1000.0));

      return result;
    } catch (Exception e) {
      long errorTime = System.currentTimeMillis();
      LOG.severe(String.format("Failed to get OpenSearch table metadata for [%s] after %.3f seconds: %s",
              String.join(",", indexNames),
              (errorTime - startTime) / 1000.0,
              e.getMessage()));

      throw new IllegalStateException("Failed to get OpenSearch table metadata for " +
              String.join(",", indexNames), e);
    }
  }
}
