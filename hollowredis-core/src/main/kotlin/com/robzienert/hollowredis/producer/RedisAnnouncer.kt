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
import redis.clients.jedis.Jedis

class RedisAnnouncer(
  private val jedis: Jedis,
  private val namespace: String
) : HollowProducer.Announcer {

  override fun announce(stateVersion: Long) {
    jedis.publish("hollow.announcements.$namespace", stateVersion.toString())
    jedis.set("hollow.announcements.$namespace.latest", stateVersion.toString())
  }
}
