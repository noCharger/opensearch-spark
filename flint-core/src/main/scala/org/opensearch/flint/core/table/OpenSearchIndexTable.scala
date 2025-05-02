/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.table

import org.json4s.{DefaultFormats, Formats, NoTypeHints}
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.jackson.JsonMethods
import org.json4s.native.Serialization
import org.opensearch.action.search.SearchRequest
import org.opensearch.client.opensearch.indices.IndicesStatsRequest
import org.opensearch.client.opensearch.indices.stats.IndicesStats
import org.opensearch.common.Strings
import org.opensearch.common.xcontent.{LoggingDeprecationHandler, XContentFactory, XContentType}
import org.opensearch.flint.core._
import org.opensearch.flint.core.storage.{FlintReader, OpenSearchClientUtils, OpenSearchSearchAfterQueryReader}
import org.opensearch.flint.core.table.OpenSearchIndexTable.maxSplitSizeBytes
import org.opensearch.search.builder.SearchSourceBuilder
import org.opensearch.search.sort.{FieldSortBuilder, SortBuilder}

import org.apache.spark.internal.Logging

/**
 * Represents an OpenSearch index.
 *
 * @param metaData
 *   MetaData containing information about the OpenSearch index.
 * @param option
 *   FlintOptions containing configuration options for the Flint client.
 */
class OpenSearchIndexTable(metaData: MetaData, option: FlintOptions) extends Table with Logging {
  @transient implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * The name of the index.
   */
  lazy val name: String = metaData.name

  /**
   * The index stats.
   */
  lazy val indexStats: IndicesStats =
    OpenSearchClientUtils
      .createClient(option)
      .stats(new IndicesStatsRequest.Builder().index(name).build())
      .indices()
      .get(name)

  /**
   * The page size for OpenSearch Rest Request.
   */
  lazy val pageSize: Int = {
    if (option.getScrollSize.isPresent) {
      option.getScrollSize.get()
    } else {
      val docCount = indexStats.primaries().docs().count()
      if (docCount == 0) {
        maxResultWindow
      } else {
        val totalSizeBytes = indexStats.primaries().store().sizeInBytes
        val docSize = Math.ceil(totalSizeBytes / docCount).toLong
        Math.max(Math.min(maxSplitSizeBytes / docSize, maxResultWindow), 1).toInt
      }
    }
  }

  /**
   * The number of shards in the index.
   */
  lazy val numberOfShards: Int = {
    if (option.supportShard()) {
      (JsonMethods.parse(metaData.setting) \ "index.number_of_shards").extract[String].toInt
    } else {
      1
    }
  }

  /**
   * The maximum result window for the index.
   */
  lazy val maxResultWindow: Int = {
    (JsonMethods.parse(metaData.setting) \ "index.max_result_window") match {
      case JString(value) => value.toInt
      case _ => 10000
    }
  }

  /**
   * Slices the table.
   */
  override def slice(): Seq[Table] = {
    Range(0, numberOfShards).map(shardId =>
      new OpenSearchIndexShardTable(metaData, option, shardId))
  }

  /**
   * Creates a reader for the table. Not supported for OpenSearchIndexTable.
   *
   * @param query
   *   The query string.
   * @return
   *   A FlintReader instance.
   */
  override def createReader(query: String, sortClauses: String, limit: Int): FlintReader = {

//    logInfo("sortClauses: " + sortClauses)

    val sourceBuilder = new SearchSourceBuilder()
      .query(Table.queryBuilder(query))
      .size(if (limit > 0) limit else pageSize)

    // Add custom sort if provided, otherwise use default sorts
    if (!Strings.isNullOrEmpty(sortClauses)) {
      try {
        implicit val formats: DefaultFormats.type = DefaultFormats

        JsonMethods.parse(sortClauses) match {
          case JArray(sortOrders) =>
            sortOrders.foreach {
              case obj: JObject =>
                // Each sort object should have one field as key
                obj.obj.headOption.foreach { case (fieldName, details) =>
                  val order = (details \ "order") match {
                    case JString("asc") => org.opensearch.search.sort.SortOrder.ASC
                    case _ => org.opensearch.search.sort.SortOrder.DESC
                  }

                  val fieldSort = new FieldSortBuilder(fieldName).order(order)

                  // Add missing parameter if present
                  (details \ "missing").toOption.foreach {
                    case JString(missing) => fieldSort.missing(missing)
                    case _ => // ignore other types
                  }

                  sourceBuilder.sort(fieldSort)
                }
              case _ => // ignore other types
            }
          case _ =>
            logWarning("Invalid sort clauses format, falling back to default sort")
            addDefaultSort(sourceBuilder)
        }
      } catch {
        case e: Exception =>
          logError("Error parsing sort clauses", e)
          addDefaultSort(sourceBuilder)
      }
    } else {
      addDefaultSort(sourceBuilder)
    }

//    logInfo("Source Builder: " + sourceBuilder.toString)

    new OpenSearchSearchAfterQueryReader(
      OpenSearchClientUtils.createClient(option),
      new SearchRequest()
        .indices(name)
        .source(sourceBuilder))
  }

  private def addDefaultSort(sourceBuilder: SearchSourceBuilder): Unit = {
    sourceBuilder.sort(
      new FieldSortBuilder("_doc").order(org.opensearch.search.sort.SortOrder.ASC))
    sourceBuilder.sort(
      new FieldSortBuilder("_id").order(org.opensearch.search.sort.SortOrder.ASC))
  }

  /**
   * Returns the schema of the table.
   *
   * @return
   *   A Schema instance.
   */
  override def schema(): Schema = JsonSchema(metaData.properties)

  /**
   * Returns the metadata of the table.
   *
   * @return
   *   A MetaData instance.
   */
  override def metaData(): MetaData = metaData

  /**
   * Is OpenSearch Table splittable.
   *
   * @return
   *   true if splittable, otherwise false.
   */
  override def isSplittable(): Boolean = numberOfShards > 1
}

object OpenSearchIndexTable {

  /**
   * Max OpenSearch Request Page size is 10MB.
   */
  val maxSplitSizeBytes = 10 * 1024 * 1024
}
