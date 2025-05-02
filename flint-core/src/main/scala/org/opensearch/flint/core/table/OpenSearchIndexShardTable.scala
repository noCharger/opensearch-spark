/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import org.opensearch.action.search.SearchRequest
import org.opensearch.common.Strings
import org.opensearch.common.xcontent.{LoggingDeprecationHandler, XContentFactory, XContentType}
import org.opensearch.flint.core.{FlintOptions, MetaData, Table}
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchClientUtils, OpenSearchSearchAfterQueryReader}
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.{FieldSortBuilder, SortBuilder, SortOrder}

/**
 * Represents an OpenSearch index shard.
 *
 * @param metaData
 *   MetaData containing information about the OpenSearch index.
 * @param option
 *   FlintOptions containing configuration options for the Flint client.
 * @param shardId
 *   Shard Id.
 */
class OpenSearchIndexShardTable(metaData: MetaData, option: FlintOptions, shardId: Int)
    extends OpenSearchIndexTable(metaData, option) {

  override def slice(): Seq[Table] = {
    throw new UnsupportedOperationException("Can't slice OpenSearchIndexShardTable")
  }

  class OpenSearchIndexShardTable(metaData: MetaData, option: FlintOptions, shardId: Int)
      extends OpenSearchIndexTable(metaData, option) {

    override def slice(): Seq[Table] = {
      throw new UnsupportedOperationException("Can't slice OpenSearchIndexShardTable")
    }

    override def createReader(
        query: String,
        sortClauses: String = "",
        limit: Int = -1): FlintReader = {

      val sourceBuilder = new SearchSourceBuilder()
        .query(Table.queryBuilder(query))
        .size(if (limit > 0) limit else pageSize)

      // Add custom sort if provided, otherwise use default sorts
      if (!Strings.isNullOrEmpty(sortClauses)) {
        // Parse the JSON sort clauses and add them to the source builder
        val parser = XContentFactory
          .xContent(XContentType.JSON)
          .createParser(Table.xContentRegistry, LoggingDeprecationHandler.INSTANCE, sortClauses)

        // Parse the sort builders from the content
        val sortBuilders = SortBuilder.fromXContent(parser)
        // Add each sort builder to the source builder
        sortBuilders.forEach(sort => sourceBuilder.sort(sort))
      } else {
        // Default sorting
        sourceBuilder.sort(
          new FieldSortBuilder("_doc").order(org.opensearch.search.sort.SortOrder.ASC))
        sourceBuilder.sort(
          new FieldSortBuilder("_id").order(org.opensearch.search.sort.SortOrder.ASC))
      }

      new OpenSearchSearchAfterQueryReader(
        OpenSearchClientUtils.createClient(option),
        new SearchRequest()
          .indices(name)
          .source(sourceBuilder)
          .preference(s"_shards:$shardId"))
    }
  }
}
