/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class containing utility methods common to multiple testing files.
 */
public class TestUtils {

  public static String readFile(Path path) {
    String fileText = "";
    try {
      fileText = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      assertTrue(false, "File " + path.toAbsolutePath().toString()
          + " doesn't exist.");
    }
    // Windows line ending conversion.
    fileText = fileText.replaceAll("\\r\\n", "\n");
    fileText = fileText.replaceAll("\\r", "\n");
    return fileText;
  }
}
