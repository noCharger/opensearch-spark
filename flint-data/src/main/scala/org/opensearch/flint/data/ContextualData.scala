/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.data

trait ContextualData {
  var context: Map[String, Any] = Map.empty

  def add(key: String, value: Any): Unit = {
    context += (key -> value)
  }

  def get(key: String): Option[Any] = {
    context.get(key)
  }
}
