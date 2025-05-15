/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.opensearch.flint.core.storage.FlintReader

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptionsInRead}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, FailureSafeParser}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.datatype.FlintDataType.DATE_FORMAT_PARAMETERS
import org.apache.spark.sql.flint.json.FlintJacksonParser
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * OpenSearchPartitionReader. Todo, add partition support.
 * @param tableName
 *   tableName
 * @param schema
 *   schema
 */
class FlintPartitionReader(reader: FlintReader, schema: StructType, options: FlintSparkConf)
    extends PartitionReader[InternalRow]
    with Logging {

  private var recordsRead: Long = 0
  private var fetchTimeMs: Long = 0
  private var parseTimeMs: Long = 0

  lazy val parser = new FlintJacksonParser(
    schema,
    new JSONOptionsInRead(CaseInsensitiveMap(DATE_FORMAT_PARAMETERS), options.timeZone, ""),
    allowArrayAsStructs = true)

  lazy val stringParser: (JsonFactory, String) => JsonParser =
    CreateJacksonParser.string(_: JsonFactory, _: String)

  lazy val safeParser = new FailureSafeParser[String](
    input => parser.parse(input, stringParser, UTF8String.fromString),
    parser.options.parseMode,
    schema,
    parser.options.columnNameOfCorruptRecord)

  var rows: Iterator[InternalRow] = Iterator.empty

  override def next: Boolean = {
    if (rows.hasNext) {
      true
    } else if (reader.hasNext) {
      // Measure fetch time
      val fetchStart = System.currentTimeMillis()
      val jsonString = reader.next()
      fetchTimeMs += System.currentTimeMillis() - fetchStart

      // Measure parse time
      val parseStart = System.currentTimeMillis()
      rows = safeParser.parse(jsonString)
      parseTimeMs += System.currentTimeMillis() - parseStart

      if (rows.hasNext) {
        recordsRead += 1
        if (recordsRead % 1000 == 0) {
          logInfo(
            f"Progress: processed $recordsRead records (fetch: ${fetchTimeMs / 1000.0}%.3fs, parse: ${parseTimeMs / 1000.0}%.3fs)")
        }
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def get(): InternalRow = {
    rows.next()
  }

  override def close(): Unit = {
    logInfo(
      f"FlintPartitionReader completed: $recordsRead records processed" +
        f" (fetch: ${fetchTimeMs / 1000.0}%.3fs, parse: ${parseTimeMs / 1000.0}%.3fs," +
        f" avg fetch: ${fetchTimeMs.toDouble / recordsRead}%.3fms/record," +
        f" avg parse: ${parseTimeMs.toDouble / recordsRead}%.3fms/record)")
    reader.close()
  }
}
