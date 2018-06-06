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
package com.robzienert.hollowredis.example.api;

import com.netflix.hollow.api.codegen.HollowAPIGenerator;
import com.netflix.hollow.core.write.HollowWriteStateEngine;
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper;

import java.io.File;
import java.io.IOException;

public class APIGenerator {

  private static final String GENERATED_API_NAME = "MovieAPI";

  private static final String DATA_MODEL_PACKAGE = "com.robzienert.hollowredis.example.model";
  private static final String GENERATED_API_PACKAGE = "com.robzienert.hollowredis.example.api.generated";

  private static final String DATA_MODEL_SOURCE_FOLDER = "src/main/java/" + DATA_MODEL_PACKAGE.replace('.', '/');
  private static final String GENERATED_API_CODE_FOLDER = "src/main/java/" + GENERATED_API_PACKAGE.replace('.', '/');

  /**
   * Run this main method to (re)generate the API based on the POJOs defining the data model.
   * <p>
   * If the first arg is populated, it will specify the root project folder.
   * If not, we will attempt to discover the root project folder.
   */
  public static void main(String args[]) throws ClassNotFoundException, IOException {
    File projectRootFolder;

    if (args.length > 0)
      projectRootFolder = new File(args[0]);
    else
      throw new IllegalArgumentException("First program arg must be path to hollowredis-example directory");


    APIGenerator generator = new APIGenerator(projectRootFolder);

    generator.generateFiles();
  }


  private final File projectRootFolder;

  public APIGenerator(File projectRootFolder) {
    this.projectRootFolder = projectRootFolder;
  }

  /**
   * Generate the API.
   */
  public void generateFiles() throws IOException, ClassNotFoundException {
    /// we'll use the following write state engine and object mapper to build schemas
    HollowWriteStateEngine stateEngine = new HollowWriteStateEngine();
    HollowObjectMapper mapper = new HollowObjectMapper(stateEngine);

    /// iterate over all java POJO files describing the data model.
    for (String filename : findProjectFolder(DATA_MODEL_SOURCE_FOLDER).list()) {
      if (filename.endsWith(".java") && !filename.equals("SourceDataRetriever.java")) {
        String discoveredType = filename.substring(0, filename.indexOf(".java"));
        /// initialize the schema for that data model type.
        mapper.initializeTypeState(Class.forName(DATA_MODEL_PACKAGE + "." + discoveredType));
      }
    }

    HollowAPIGenerator codeGenerator = new HollowAPIGenerator.Builder()
      .withAPIClassname(GENERATED_API_NAME)
      .withPackageName(GENERATED_API_PACKAGE)
      .withDataModel(stateEngine)
      .build();

    File apiCodeFolder = findProjectFolder(GENERATED_API_CODE_FOLDER);

    apiCodeFolder.mkdirs();

    for (File f : apiCodeFolder.listFiles())
      f.delete();

    codeGenerator.generateFiles(apiCodeFolder);
  }

  /**
   * Find the relative project folder
   */
  private File findProjectFolder(String projectFolder) {
    File f = projectRootFolder;

    for (String s : projectFolder.split("//")) {
      f = new File(f, s);
    }

    return f;
  }
}
