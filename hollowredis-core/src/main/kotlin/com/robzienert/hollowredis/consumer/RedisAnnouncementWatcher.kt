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

import com.netflix.hollow.api.HollowConstants.VERSION_NONE
import com.netflix.hollow.api.consumer.HollowConsumer
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import kotlin.concurrent.thread

class RedisAnnouncementWatcher(
  private val jedis: Jedis,
  private val pollingJedis: Jedis,
  private val namespace: String
) : JedisPubSub(), HollowConsumer.AnnouncementWatcher {

  private val consumers: MutableList<HollowConsumer> = mutableListOf()

  private val t = thread(name = "hollow-redis-announcementwatcher", start = true, isDaemon = true) {
    jedis.subscribe(this, "hollow.announcements.$namespace")
  }

  // Not sure if this is actually needed at all...
  private val t2 = thread(name = "hollow-redis-announcementwatcher-poller", start = true, isDaemon = true) {
    while (true) {
      pollingJedis.get("hollow.announcements.$namespace.latest")?.toLong()?.also { onAnnouncement(it) }
      Thread.sleep(1000)
    }
  }

  private var currentVersion: Long = VERSION_NONE

  override fun onMessage(channel: String, message: String) {
    onAnnouncement(message.toLong())
  }

  private fun onAnnouncement(latestVersion: Long) {
    if (latestVersion != currentVersion) {
      currentVersion = latestVersion
      consumers.forEach { it.triggerAsyncRefresh() }
    }
  }

  override fun getLatestVersion(): Long = currentVersion

  override fun subscribeToUpdates(consumer: HollowConsumer) {
    consumers.add(consumer)
  }
}
