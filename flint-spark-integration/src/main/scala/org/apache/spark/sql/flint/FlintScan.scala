/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain

import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.types.StructType

case class FlintScan(
    tables: Seq[org.opensearch.flint.core.Table],
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate],
    pushedSortOrders: Array[SortOrder],
    pushedLimit: Int)
    extends Scan
    with Batch {

  override def readSchema(): StructType = schema

  override def planInputPartitions(): Array[InputPartition] = {
    tables
      .flatMap(table => {
        if (table.isSplittable()) {
          table.slice().map(table => OpenSearchSplit(table))
        } else {
          Seq(OpenSearchSplit(table))
        }
      })
      .toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    FlintPartitionReaderFactory(schema, options, pushedPredicates, pushedSortOrders, pushedLimit)
  }

  override def toBatch: Batch = this

  /**
   * Print pushedPredicates when explain(mode="extended"). Learn from SPARK JDBCScan.
   */
  override def description(): String = {
    val desc = super.description() + ", PushedPredicates: " + pushedPredicates
      .map {
        case p if p.name().equalsIgnoreCase(BloomFilterMightContain.NAME) => p.name()
        case p => p.toString()
      }
      .mkString("[", ", ", "]")

    if (pushedSortOrders.nonEmpty) {
      desc + ", PushedSort: " + pushedSortOrders.mkString("[", ", ", "]") +
        (if (pushedLimit > 0) s", PushedLimit: $pushedLimit" else "")
    } else {
      desc
    }
  }
}

/**
 * Each OpenSearchSplit is backed by an OpenSearch index table.
 *
 * @param table
 *   {@link org.opensearch.flint.core.Table}
 */
private[spark] case class OpenSearchSplit(table: org.opensearch.flint.core.Table)
    extends InputPartition {}
