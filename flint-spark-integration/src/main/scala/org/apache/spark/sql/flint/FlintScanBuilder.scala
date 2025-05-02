/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.opensearch.flint.spark.skipping.bloomfilter.BloomFilterMightContain

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownTopN, SupportsPushDownV2Filters}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintScanBuilder(
    tables: Seq[org.opensearch.flint.core.Table],
    schema: StructType,
    options: FlintSparkConf)
    extends ScanBuilder
    with SupportsPushDownV2Filters
    with SupportsPushDownTopN
    with Logging {

  private var pushedPredicate = Array.empty[Predicate]
  private var pushedSortOrders = Array.empty[SortOrder]
  private var pushedLimit: Int = -1

  override def build(): Scan = {
    FlintScan(tables, schema, options, pushedPredicate, pushedSortOrders, pushedLimit)
  }

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (pushed, unSupported) =
      predicates.partition(FlintQueryCompiler(schema).compile(_).nonEmpty)
    pushedPredicate = pushed
    unSupported
  }

  override def pushedPredicates(): Array[Predicate] = pushedPredicate
    .filterNot(_.name().equalsIgnoreCase(BloomFilterMightContain.NAME))

  override def pushTopN(orders: Array[SortOrder], limit: Int): Boolean = {
    pushedSortOrders = orders
    pushedLimit = limit
    true
  }
}
