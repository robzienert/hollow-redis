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
package com.robzienert.hollowredis.consumer

import com.netflix.hollow.api.consumer.HollowConsumer
import com.netflix.hollow.api.consumer.HollowConsumer.Blob
import com.netflix.hollow.api.producer.HollowProducer.Blob.Type
import com.netflix.hollow.api.producer.HollowProducer.Blob.Type.*
import com.robzienert.hollowredis.HollowRedisUtil.getObjectName
import com.robzienert.hollowredis.HollowRedisUtil.getSnapshotIndexKey
import redis.clients.jedis.BinaryJedis
import java.nio.ByteBuffer
import java.nio.charset.Charset

class RedisBlobRetriever(
  private val jedis: BinaryJedis,
  private val namespace: String
) : HollowConsumer.BlobRetriever {

  override fun retrieveSnapshotBlob(desiredVersion: Long): Blob? {
    val blob = knownSnapshotBlob(desiredVersion)
    if (blob != null) {
      return blob
    }

    // No exact match for the snapshot leading to the desired state.
    // Use the snapshot index to find the nearest one before the desired state.
    val snapshotIndex = jedis.lrange(getSnapshotIndexKey(namespace).toByteArray(), 0, Long.MAX_VALUE)
    var pos = 0L
    var currentSnapshotStateId = 0L
    while (pos < snapshotIndex.size) {
      val next = snapshotIndex[(currentSnapshotStateId + pos).toInt()].toLong()
      if (next > desiredVersion) {
        if (currentSnapshotStateId == 0L) {
          return null
        }
        return knownSnapshotBlob(currentSnapshotStateId)
      }
      currentSnapshotStateId = next
      pos += 1
    }
    if (currentSnapshotStateId != 0L) {
      return knownSnapshotBlob(currentSnapshotStateId)
    }
    return null
  }

  override fun retrieveDeltaBlob(currentVersion: Long): Blob? {
    return knownDeltaBlob(DELTA, currentVersion)
  }

  override fun retrieveReverseDeltaBlob(currentVersion: Long): Blob? {
    return knownDeltaBlob(REVERSE_DELTA, currentVersion)
  }

  private fun knownSnapshotBlob(desiredVersion: Long): Blob? {
    val objectName = getObjectName(namespace, SNAPSHOT, desiredVersion)
    return jedis.hmget(objectName.toByteArray(), "to_state".toByteArray(), "blob".toByteArray())
      ?.let { fields ->
        StringBlob(fields[0].toLong(), fields[1].toString(Charset.forName("UTF-8")))
      }
  }

  private fun knownDeltaBlob(type: Type, fromVersion: Long): Blob? {
    val objectName = getObjectName(namespace, type, fromVersion)
    return jedis.hmget(objectName.toByteArray(), "from_state".toByteArray(), "to_state".toByteArray(), "blob".toByteArray())
      ?.let { fields ->
        StringBlob(fields[0].toLong(), fields[1].toLong(), fields[2].toString(Charset.forName("UTF-8")))
      }
  }

  private fun ByteArray.toLong(): Long {
    return ByteBuffer.wrap(this).long
  }
}
