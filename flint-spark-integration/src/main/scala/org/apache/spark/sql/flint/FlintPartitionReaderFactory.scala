/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.storage.FlintQueryCompiler
import org.apache.spark.sql.types.StructType

case class FlintPartitionReaderFactory(
    schema: StructType,
    options: FlintSparkConf,
    pushedPredicates: Array[Predicate],
    pushedSortOrders: Array[SortOrder],
    pushedLimit: Int)
    extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val queryCompiler = FlintQueryCompiler(schema)
    val query = queryCompiler.compile(pushedPredicates)
    val sortClauses = queryCompiler.compileSortOrders(pushedSortOrders)

    new FlintPartitionReader(
      partition.asInstanceOf[OpenSearchSplit].table.createReader(query, sortClauses, pushedLimit),
      schema,
      options)
  }
}
