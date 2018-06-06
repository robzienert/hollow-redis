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
import com.netflix.hollow.explorer.ui.jetty.HollowExplorerUIServer;
import com.netflix.hollow.history.ui.jetty.HollowHistoryUIServer;
import com.robzienert.hollowredis.consumer.RedisAnnouncementWatcher;
import com.robzienert.hollowredis.consumer.RedisBlobRetriever;
import com.robzienert.hollowredis.example.api.generated.*;
import redis.clients.jedis.Jedis;

public class Consumer {

  public static void main(String args[]) throws Exception {
    HollowConsumer.BlobRetriever blobRetriever = new RedisBlobRetriever(new Jedis(), "myNamespace");
    HollowConsumer.AnnouncementWatcher announcementWatcher = new RedisAnnouncementWatcher(new Jedis(), new Jedis(), "myNamespace");

    HollowConsumer consumer = HollowConsumer
      .withBlobRetriever(blobRetriever)
      .withAnnouncementWatcher(announcementWatcher)
      .withGeneratedAPIClass(MovieAPI.class)
      .build();

    consumer.triggerRefresh();

    hereIsHowToUseTheDataProgrammatically(consumer);

    /// start a history server on port 7777
    HollowHistoryUIServer historyServer = new HollowHistoryUIServer(consumer, 7777);
    historyServer.start();

    /// start an explorer server on port 7778
    HollowExplorerUIServer explorerServer = new HollowExplorerUIServer(consumer, 7778);
    explorerServer.start();

    historyServer.join();
  }

  private static void hereIsHowToUseTheDataProgrammatically(HollowConsumer consumer) {
    /// create an index for Movie based on its primary key (Id)
    MoviePrimaryKeyIndex idx = new MoviePrimaryKeyIndex(consumer);
    /// create an index for movies by the names of cast members
    MovieAPIHashIndex moviesByActorName = new MovieAPIHashIndex(consumer, "Movie", "", "actors.element.actorName.value");

    /// find the movie for a some known ID
    Movie foundMovie = idx.findMatch(1000004);

    /// for each actor in that movie
    for(Actor actor : foundMovie.getActors()) {
      /// get all of movies of which they are cast members
      for(Movie movie : moviesByActorName.findMovieMatches(actor.getActorName().getValue())) {
        /// and just print the result
        System.out.println(actor.getActorName().getValue() + " starred in " + movie.getTitle().getValue());
      }
    }
  }

}
