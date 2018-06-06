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
import java.io.BufferedInputStream
import java.io.InputStream

internal class StringBlob : HollowConsumer.Blob {

  private val blob: String

  constructor(toVersion: Long, blob: String) : super(toVersion) {
    this.blob = blob
  }

  constructor(fromVersion: Long, toVersion: Long, blob: String) : super(fromVersion, toVersion) {
    this.blob = blob
  }

  override fun getInputStream(): InputStream {
    return BufferedInputStream(blob.byteInputStream())
  }
}
