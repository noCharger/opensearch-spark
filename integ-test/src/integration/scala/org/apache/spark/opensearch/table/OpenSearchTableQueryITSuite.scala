/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.opensearch.table

import org.opensearch.flint.spark.ppl.FlintPPLSuite
import org.opensearch.flint.spark.udt.{GeoPoint, IPAddress, IPFunctions}

import org.apache.spark.sql.{DataFrame, ExplainSuiteHelper, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * Test queries on OpenSearch Table
 */
class OpenSearchTableQueryITSuite
    extends OpenSearchCatalogSuite
    with FlintPPLSuite
    with ExplainSuiteHelper {
  test("SQL Join two indices") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)
        val df = spark.sql(s"""
        SELECT t1.accountId, t2.eventName, t2.eventSource
        FROM ${catalogName}.default.$indexName1 as t1 JOIN ${catalogName}.default.$indexName2 as t2 ON
        t1.accountId == t2.accountId""")

        checkAnswer(df, Row("123", "event", "source"))
      }
    }
  }

  test("PPL Lookup") {
    val indexName1 = "t0001"
    val indexName2 = "t0002"
    val factTbl = s"${catalogName}.default.$indexName1"
    val lookupTbl = s"${catalogName}.default.$indexName2"
    withIndexName(indexName1) {
      withIndexName(indexName2) {
        simpleIndex(indexName1)
        simpleIndex(indexName2)

        val df = spark.sql(
          s"source = $factTbl | stats count() by accountId " +
            s"| LOOKUP $lookupTbl accountId REPLACE eventSource")
        checkAnswer(df, Row(1, "123", "source"))
      }
    }
  }

  test("Query index with alias data type") {
    val index1 = "t0001"
    withIndexName(index1) {
      indexWithAlias(index1)
      // select original field and alias field
      var df = spark.sql(s"""SELECT id, alias FROM ${catalogName}.default.$index1""")
      checkAnswer(df, Seq(Row(1, 1), Row(2, 2)))

      // filter on alias field
      df = spark.sql(s"""SELECT id, alias FROM ${catalogName}.default.$index1 WHERE alias=1""")
      checkAnswer(df, Row(1, 1))

      // filter on original field
      df = spark.sql(s"""SELECT id, alias FROM ${catalogName}.default.$index1 WHERE id=1""")
      checkAnswer(df, Row(1, 1))
    }
  }

  test("Query multi-field index - Exact match on multi-field is pushed down") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"
    withIndexName(indexName) {
      indexMultiFields(indexName)
      // Validate that an exact equality query on the multi-field 'aTextString'
      // is pushed down to OpenSearch and returns the matching document.
      var df =
        spark.sql(s"""SELECT id FROM $table WHERE aTextString = "Treviso-Sant'Angelo Airport" """)
      checkPushedInfo(df, "aTextString IS NOT NULL, aTextString = 'Treviso-Sant'Angelo Airport'")
      checkAnswer(df, Seq(Row(1)))

      // Validate that an equality query on 'aTextString' with a non-exact match returns no results.
      df = spark.sql(s"""SELECT id FROM $table WHERE aTextString = "Airport" """)
      checkPushedInfo(df, "aTextString IS NOT NULL, aTextString = 'Airport'")
      checkAnswer(df, Seq())
    }
  }

  test("Query text fields index - Combined conditions on text and keyword fields") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"
    withIndexName(indexName) {
      indexMultiFields(indexName)

      // Validate that the condition on the text field 'aText', which is evaluated by Spark
      // without push down to OpenSearch, returns the expected document
      var df =
        spark.sql(s"""SELECT id FROM $table WHERE aText = "Treviso-Sant'Angelo Airport" """)
      checkPushedInfo(df, "aText IS NOT NULL")
      checkAnswer(df, Seq(Row(1)))

      // Validate that the condition on the text field 'aText', which is evaluated by Spark,
      // aString is push down to OpenSearch returns the expected document when both conditions are met.
      df = spark.sql(
        s"""SELECT id FROM $table WHERE aText = "Treviso-Sant'Angelo Airport" AND aString = "OpenSearch-Air" """)
      checkPushedInfo(df, "aText IS NOT NULL, aString IS NOT NULL, aString = 'OpenSearch-Air'")
      checkAnswer(df, Seq(Row(1)))

      // Validate that a query with a full string equality condition on 'aText'
      // that does not exactly match returns no results.
      df = spark.sql(s"""SELECT id FROM $table WHERE aText = "Airport" """)
      checkAnswer(df, Seq())
    }
  }

  test("Query geo_point field") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"
    withIndexName(indexName) {
      indexGeoPointFields(indexName)

      val df =
        spark.sql(s"""SELECT id, location FROM $table""")
      df.explain(true)
      checkAnswer(
        df,
        Seq(
          Row(1, GeoPoint(40.12, -71.34)),
          Row(2, GeoPoint(40.12, -71.34)),
          Row(3, GeoPoint(40.078125, -71.3671875)), // different value due to precision
          Row(4, GeoPoint(40.12, -71.34)),
          Row(5, GeoPoint(40.12, -71.34))))
    }
  }

  test("Query with sort and limit pushdown on timestamp") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    withIndexName(indexName) {
      // Create an index with multiple documents including timestamp
      createIndex(
        indexName,
        """
          |{
          |  "properties": {
          |    "id": { "type": "integer" },
          |    "@timestamp": {
          |        "type": "date",
          |        "format": "strict_date_optional_time||epoch_millis"
          |    },
          |    "name": { "type": "keyword" }
          |  }
          |}
          |""".stripMargin,
        """{"id": 1, "@timestamp": "2024-01-01T00:00:00Z", "name": "a"}""",
        """{"id": 2, "@timestamp": "2024-01-02T00:00:00Z", "name": "b"}""",
        """{"id": 3, "@timestamp": "2024-01-03T00:00:00Z", "name": "c"}""",
        """{"id": 4, "@timestamp": "2024-01-04T00:00:00Z", "name": "d"}""",
        """{"id": 5, "@timestamp": "2024-01-05T00:00:00Z", "name": "e"}""")

      // Test case 1: Sort by timestamp with limit
      var df = spark.sql(s"""
           |SELECT id, `@timestamp`, name
           |FROM $table
           |ORDER BY `@timestamp` DESC
           |LIMIT 3
           |""".stripMargin)

      // Verify sort and limit are pushed down
      checkPushedInfo(df, "Limit 3", "@timestamp DESC")
      checkAnswer(
        df,
        Seq(
          Row(5, java.sql.Timestamp.valueOf("2024-01-05 00:00:00"), "e"),
          Row(4, java.sql.Timestamp.valueOf("2024-01-04 00:00:00"), "d"),
          Row(3, java.sql.Timestamp.valueOf("2024-01-03 00:00:00"), "c")))

      // Test case 2: Sort by timestamp and name
      df = spark.sql(s"""
           |SELECT id, `@timestamp`, name
           |FROM $table
           |WHERE `@timestamp` > '2024-01-02T00:00:00Z'
           |ORDER BY name ASC, `@timestamp` DESC
           |""".stripMargin)

      // Verify filter and sort are pushed down
      checkPushedInfo(df, "@timestamp > '2024-01-02T00:00:00Z'", "name ASC", "@timestamp DESC")
      checkAnswer(
        df,
        Seq(
          Row(3, java.sql.Timestamp.valueOf("2024-01-03 00:00:00"), "c"),
          Row(4, java.sql.Timestamp.valueOf("2024-01-04 00:00:00"), "d"),
          Row(5, java.sql.Timestamp.valueOf("2024-01-05 00:00:00"), "e")))

      // Test case 3: Sort with timestamp filter and limit
      df = spark.sql(s"""
           |SELECT id, `@timestamp`, name
           |FROM $table
           |WHERE `@timestamp` > '2024-01-01T00:00:00Z'
           |ORDER BY `@timestamp` ASC
           |LIMIT 2
           |""".stripMargin)

      // Verify filter, sort and limit are pushed down
      checkPushedInfo(df, "@timestamp > '2024-01-01T00:00:00Z'", "@timestamp ASC", "Limit 2")
      checkAnswer(
        df,
        Seq(
          Row(2, java.sql.Timestamp.valueOf("2024-01-02 00:00:00"), "b"),
          Row(3, java.sql.Timestamp.valueOf("2024-01-03 00:00:00"), "c")))
    }
  }

  def createIndex(indexName: String, mappings: String, docs: String*): Unit = {
    index(indexName, oneNodeSetting, mappings, docs)
  }

  def checkPushedInfo(df: DataFrame, expectedPlanFragment: String*): Unit = {
    df.queryExecution.optimizedPlan.collect { case _: DataSourceV2ScanRelation =>
      checkKeywordsExistsInExplain(df, expectedPlanFragment: _*)
    }
  }

  test("Query index with half_float data type") {
    val indexName = "t0001"
    val table = s"${catalogName}.default.$indexName"
    withIndexName(indexName) {
      indexWithNumericFields(indexName)

      var df = spark.sql(s"""SELECT id, floatField, halfFloatField FROM ${table}""")
      checkAnswer(df, Seq(Row(1, 1.1f, 1.2f), Row(2, 2.1f, 2.2f)))

      df = spark.sql(
        s"""SELECT id, floatField, halfFloatField FROM ${table} WHERE halfFloatField < 2.0""")
      checkPushedInfo(df, "halfFloatField IS NOT NULL, halfFloatField < 2.0")
      checkAnswer(df, Seq(Row(1, 1.1f, 1.2f)))
    }
  }

  test("Query index with ip data type") {
    val index1 = "t0001"
    val tableName = s"""$catalogName.default.$index1"""
    val spark = SparkSession.builder().getOrCreate()
    IPFunctions.registerFunctions(spark)
    val clientIp: Array[String] = Array("192.168.0.10", "192.168.0.11", "::ffff:192.168.0.10")
    val serverIp: Array[String] = Array("100.10.12.123", "::ffff:100.10.12.123")

    withIndexName(index1) {
      indexWithIp(index1)

      var df: DataFrame = null

      df = spark.sql(s"SELECT client, server FROM $tableName")
      checkAnswer(
        df,
        Seq(
          Row(IPAddress(clientIp(0)), IPAddress(serverIp(0))),
          Row(IPAddress(clientIp(1)), IPAddress(serverIp(0))),
          Row(IPAddress(clientIp(2)), IPAddress(serverIp(1)))))

      df = spark.sql(
        s"SELECT client, server FROM $tableName WHERE cidrmatch(client, '192.168.0.10/32')")
      checkAnswer(df, Seq(Row(IPAddress(clientIp(0)), IPAddress(serverIp(0)))))

      df = spark.sql(
        s"SELECT client, server FROM $tableName WHERE cidrmatch(client, '192.168.0.0/24')")
      checkAnswer(
        df,
        Seq(
          Row(IPAddress(clientIp(0)), IPAddress(serverIp(0))),
          Row(IPAddress(clientIp(1)), IPAddress(serverIp(0)))))

      df = spark.sql(
        s"SELECT client, server FROM $tableName WHERE cidrmatch(client, '192.168.0.0/255.255.255.0')")
      checkAnswer(
        df,
        Seq(
          Row(IPAddress(clientIp(0)), IPAddress(serverIp(0))),
          Row(IPAddress(clientIp(1)), IPAddress(serverIp(0)))))

      df = spark.sql(
        s"SELECT client, server FROM $tableName WHERE cidrmatch(client, '::ffff:192.168.0.10/128')")
      checkAnswer(df, Seq(Row(IPAddress(clientIp(2)), IPAddress(serverIp(1)))))

      df = spark.sql(
        s"SELECT client, server FROM $tableName WHERE cidrmatch(client, '::ffff:192.168.0.0/120')")
      checkAnswer(df, Seq(Row(IPAddress(clientIp(2)), IPAddress(serverIp(1)))))
    }
  }
}
