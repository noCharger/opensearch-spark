/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.data.FlintCommand

trait CommandLifecycleManager {
  def setWriter(writer: REPLWriter): Unit
  def setLocalProperties(commandDetails: FlintCommand): Unit
  def prepareCommandLifecycle(): Either[String, Unit]
  def closeCommandLifecycle(): Unit
  def getNextCommand(sessionId: String, commandState: String): Option[FlintCommand]
  def updateCommandDetails(commandDetails: FlintCommand): Unit
}
