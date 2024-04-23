/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{ExecutionContext, Future, TimeoutException}
import scala.concurrent.duration.{Duration, MINUTES}
import scala.util.{Failure, Success, Try}

import org.opensearch.flint.core.metrics.MetricConstants
import org.opensearch.flint.core.metrics.MetricsUtil.incrementCounter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.util.ShuffleCleaner
import org.apache.spark.util.ThreadUtils

trait JobExecution {
  def start(): Unit
  def stop(): Unit
}

abstract class JobOperator(spark: SparkSession) extends JobExecution with Logging {
  def start(): Unit
  def stop(): Unit = {
    Try {
      spark.stop()
      logInfo("stopped spark session")
    } match {
      case Success(_) =>
      case Failure(e) => logError("unexpected error while stopping spark session", e)
    }
  }
}

object JobOperatorFactory {
  def apply(
      spark: SparkSession,
      query: String,
      queryId: String,
      dataSource: String,
      resultIndex: String,
      streaming: Boolean,
      streamingRunningCount: AtomicInteger): JobOperator = {
    // TODO: Refactor this hardcode logic to support custom job operator
    if (queryId.startsWith("cwl-")) {
      new CWLJobOperator(spark, query, queryId, dataSource)
    } else {
      new FlintJobOperator(
        spark,
        query,
        dataSource,
        resultIndex,
        streaming,
        streamingRunningCount)
    }
  }
}

class FlintJobOperator(
    spark: SparkSession,
    query: String,
    dataSource: String,
    resultIndex: String,
    streaming: Boolean,
    streamingRunningCount: AtomicInteger)
    extends JobOperator(spark)
    with FlintJobExecutor {
  // JVM shutdown hook
  sys.addShutdownHook(stop())
  def start(): Unit = {
    val threadPool = ThreadUtils.newDaemonFixedThreadPool(1, "check-create-index")
    implicit val executionContext = ExecutionContext.fromExecutor(threadPool)

    var dataToWrite: Option[DataFrame] = None

    val startTime = System.currentTimeMillis()
    streamingRunningCount.incrementAndGet()

    // osClient needs spark session to be created first to get FlintOptions initialized.
    // Otherwise, we will have connection exception from EMR-S to OS.
    val osClient = new OSClient(FlintSparkConf().flintOptions())
    var exceptionThrown = true
    try {
      val futureMappingCheck = Future {
        checkAndCreateIndex(osClient, resultIndex)
      }
      val data = executeQuery(spark, query, dataSource, "", "", streaming)

      val mappingCheckResult = ThreadUtils.awaitResult(futureMappingCheck, Duration(1, MINUTES))
      dataToWrite = Some(mappingCheckResult match {
        case Right(_) => data
        case Left(error) =>
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider)
      })
      exceptionThrown = false
    } catch {
      case e: TimeoutException =>
        val error = s"Getting the mapping of index $resultIndex timed out"
        logError(error, e)
        dataToWrite = Some(
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider))
      case e: Exception =>
        val error = processQueryException(e, spark, dataSource, query, "", "")
        dataToWrite = Some(
          getFailedData(spark, dataSource, error, "", query, "", startTime, currentTimeProvider))
    } finally {
      cleanUpResources(exceptionThrown, threadPool, dataToWrite, resultIndex, osClient)
    }
  }
  protected def cleanUpResources(
      exceptionThrown: Boolean,
      threadPool: ThreadPoolExecutor,
      dataToWrite: Option[DataFrame],
      resultIndex: String,
      osClient: OSClient): Unit = {
    try {
      dataToWrite.foreach(df => writeDataFrameToOpensearch(df, resultIndex, osClient))
    } catch {
      case e: Exception => logError("fail to write to result index", e)
    }

    try {
      // Wait for streaming job complete if no error and there is streaming job running
      if (!exceptionThrown && streaming && spark.streams.active.nonEmpty) {
        // Clean Spark shuffle data after each microBatch.
        spark.streams.addListener(new ShuffleCleaner(spark))
        // wait if any child thread to finish before the main thread terminates
        spark.streams.awaitAnyTermination()
      }
    } catch {
      case e: Exception => logError("streaming job failed", e)
    }

    try {
      threadPool.shutdown()
      logInfo("shut down thread threadpool")
    } catch {
      case e: Exception => logError("Fail to close threadpool", e)
    }
    recordStreamingCompletionStatus(exceptionThrown)

    // Check for non-daemon threads that may prevent the driver from shutting down.
    // Non-daemon threads other than the main thread indicate that the driver is still processing tasks,
    // which may be due to unresolved bugs in dependencies or threads not being properly shut down.
    if (terminateJVM && threadPoolFactory.hasNonDaemonThreadsOtherThanMain) {
      logInfo("A non-daemon thread in the driver is seen.")
      // Exit the JVM to prevent resource leaks and potential emr-s job hung.
      // A zero status code is used for a graceful shutdown without indicating an error.
      // If exiting with non-zero status, emr-s job will fail.
      // This is a part of the fault tolerance mechanism to handle such scenarios gracefully
      System.exit(0)
    }
  }

  /**
   * Records the completion of a streaming job by updating the appropriate metrics. This method
   * decrements the running metric for streaming jobs and increments either the success or failure
   * metric based on whether an exception was thrown.
   *
   * @param exceptionThrown
   *   Indicates whether an exception was thrown during the streaming job execution.
   */
  private def recordStreamingCompletionStatus(exceptionThrown: Boolean): Unit = {
    // Decrement the metric for running streaming jobs as the job is now completing.
    if (streamingRunningCount.get() > 0) {
      streamingRunningCount.decrementAndGet()
    }

    exceptionThrown match {
      case true => incrementCounter(MetricConstants.STREAMING_FAILED_METRIC)
      case false => incrementCounter(MetricConstants.STREAMING_SUCCESS_METRIC)
    }
  }
}

class CWLJobOperator(spark: SparkSession, query: String, queryId: String, dataSource: String)
    extends JobOperator(spark) {

  def start(): Unit = {
    logInfo("Starting CWL job")
    spark.sparkContext.setJobGroup(queryId, "Job group for " + queryId, interruptOnCancel = true)
    val result: DataFrame = spark.sql(query)
    // TODO: write data frame into egress
  }
}
