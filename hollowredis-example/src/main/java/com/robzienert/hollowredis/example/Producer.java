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
package com.robzienert.hollowredis.example;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.producer.HollowProducer;
import com.robzienert.hollowredis.consumer.RedisAnnouncementWatcher;
import com.robzienert.hollowredis.consumer.RedisBlobRetriever;
import com.robzienert.hollowredis.example.model.Movie;
import com.robzienert.hollowredis.example.model.SourceDataRetriever;
import com.robzienert.hollowredis.producer.RedisAnnouncer;
import com.robzienert.hollowredis.producer.RedisPublisher;
import redis.clients.jedis.Jedis;

/**
 * Adapted from
 * https://github.com/Netflix/hollow-reference-implementation/blob/master/src/main/java/how/hollow/producer/Producer.java
 */
public class Producer {

  private static final long MIN_TIME_BETWEEN_CYCLES = 10000;

  public static void main(String args[]) {
    HollowProducer.Publisher publisher = new RedisPublisher(new Jedis(), "myNamespace");
    HollowProducer.Announcer announcer = new RedisAnnouncer(new Jedis(), "myNamespace");

    HollowConsumer.BlobRetriever blobRetriever = new RedisBlobRetriever(new Jedis(), "myNamespace");
    HollowConsumer.AnnouncementWatcher announcementWatcher = new RedisAnnouncementWatcher(new Jedis(), new Jedis(), "myNamespace");

    HollowProducer producer = HollowProducer.withPublisher(publisher)
      .withAnnouncer(announcer)
      .build();

    producer.initializeDataModel(Movie.class);

    restoreIfAvailable(producer, blobRetriever, announcementWatcher);

    cycleForever(producer);
  }

  public static void restoreIfAvailable(HollowProducer producer,
                                        HollowConsumer.BlobRetriever retriever,
                                        HollowConsumer.AnnouncementWatcher unpinnableAnnouncementWatcher) {

    System.out.println("ATTEMPTING TO RESTORE PRIOR STATE...");
    long latestVersion = unpinnableAnnouncementWatcher.getLatestVersion();
    if (latestVersion != HollowConsumer.AnnouncementWatcher.NO_ANNOUNCEMENT_AVAILABLE) {
      producer.restore(latestVersion, retriever);
      System.out.println("RESTORED " + latestVersion);
    } else {
      System.out.println("RESTORE NOT AVAILABLE");
    }
  }


  public static void cycleForever(HollowProducer producer) {
    final SourceDataRetriever sourceDataRetriever = new SourceDataRetriever();

    long lastCycleTime = Long.MIN_VALUE;
    while (true) {
      waitForMinCycleTime(lastCycleTime);
      lastCycleTime = System.currentTimeMillis();
      producer.runCycle(writeState -> {
        for (Movie movie : sourceDataRetriever.retrieveAllMovies()) {
          writeState.add(movie);  /// <-- this is thread-safe, and can be done in parallel
        }
      });
    }
  }

  private static void waitForMinCycleTime(long lastCycleTime) {
    long targetNextCycleTime = lastCycleTime + MIN_TIME_BETWEEN_CYCLES;

    while (System.currentTimeMillis() < targetNextCycleTime) {
      try {
        Thread.sleep(targetNextCycleTime - System.currentTimeMillis());
      } catch (InterruptedException ignore) { }
    }
  }

}
