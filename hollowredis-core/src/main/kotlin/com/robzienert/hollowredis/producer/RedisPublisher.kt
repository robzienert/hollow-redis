/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.robzienert.hollowredis.producer

import com.netflix.hollow.api.producer.HollowProducer
import com.netflix.hollow.api.producer.HollowProducer.Blob
import com.netflix.hollow.api.producer.HollowProducer.Blob.Type.*
import com.robzienert.hollowredis.HollowRedisUtil
import com.robzienert.hollowredis.HollowRedisUtil.getSnapshotIndexKey
import redis.clients.jedis.BinaryJedis
import java.nio.ByteBuffer

class RedisPublisher(
  private val jedis: BinaryJedis,
  private val namespace: String
) : HollowProducer.Publisher {

  override fun publish(blob: Blob) {
    when (blob.type) {
      SNAPSHOT -> publishSnapshot(blob)
      DELTA -> publishDelta(blob)
      REVERSE_DELTA -> publishReverseDelta(blob)
      else -> throw UnknownBlobType(blob.type)
    }
  }

  private fun publishSnapshot(blob: Blob) {
    jedis.hmset(blob.getObjectName().toByteArray(), mapOf(
      "to_state".toByteArray() to blob.toVersion.toByteArray(),
      "blob".toByteArray() to blob.getText()
    ))
    jedis.lpush(getSnapshotIndexKey(namespace).toByteArray(), blob.toVersion.toByteArray())
  }

  private fun publishDelta(blob: Blob) {
    jedis.hmset(blob.getObjectName().toByteArray(), mapOf(
      "from_state".toByteArray() to blob.fromVersion.toByteArray(),
      "to_state".toByteArray() to blob.toVersion.toByteArray(),
      "blob".toByteArray() to blob.getText()
    ))
  }

  private fun publishReverseDelta(blob: Blob) {
    jedis.hmset(blob.getObjectName().toByteArray(), mapOf(
      "from_state".toByteArray() to blob.fromVersion.toByteArray(),
      "to_state".toByteArray() to blob.toVersion.toByteArray(),
      "blob".toByteArray() to blob.getText()
    ))
  }

  private fun Blob.getObjectName(): String {
    val lookupVersion = when (type) {
      SNAPSHOT -> toVersion
      DELTA -> fromVersion
      REVERSE_DELTA -> fromVersion
      else -> throw UnknownBlobType(type)
    }
    return HollowRedisUtil.getObjectName(namespace, type, lookupVersion)
  }

  private fun Blob.getText(): ByteArray =
    newInputStream().bufferedReader().use { it.readText() }.toByteArray()

  private fun Long.toByteArray(): ByteArray {
    return ByteBuffer.allocate(8).putLong(this).array()
  }
}

private class UnknownBlobType(type: Blob.Type) : IllegalArgumentException("Type: $type")
