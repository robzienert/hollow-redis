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
package com.robzienert.hollowredis.example.model;

import com.netflix.hollow.core.write.objectmapper.HollowPrimaryKey;

/**
 * https://github.com/Netflix/hollow-reference-implementation/blob/master/src/main/java/how/hollow/producer/datamodel/Actor.java
 */
@HollowPrimaryKey(fields="actorId")
public class Actor {
  public int actorId;
  public String actorName;

  public Actor() { }

  public Actor(int actorId, String actorName) {
    this.actorId = actorId;
    this.actorName = actorName;
  }

}
