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
package org.apache.calcite.buildtools.parser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Queue;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Arrays;

public class DialectTraverser {

  private static final Comparator<File> fileComparator = new Comparator<File>() {
    @Override
    public int compare(File file1, File file2) {
      return Boolean.compare(file1.isDirectory(), file2.isDirectory());
    }
  };

  private final File dialectDirectory;
  private final File rootDirectory;
  private final String outputFile;
  private final DialectGenerate dialectGenerate;

  public DialectTraverser(File dialectDirectory, File rootDirectory, String outputFile) {
    this.dialectDirectory = dialectDirectory;
    this.rootDirectory = rootDirectory;
    this.outputFile = outputFile;
    this.dialectGenerate = new DialectGenerate();
  }

  /**
   * Extracts functions and prints the results for the given dialect.
   */
  public void run() {
    Map<String, String> functions = extractFunctions();
    // TODO(AndrewPochapsky): Remove this once generation logic added.
    for (Map.Entry<String, String> entry : functions.entrySet()) {
      System.out.println(entry.getKey() + "=" + entry.getValue() + "\n");
    }
  }

  /**
   * Traverses the parsing directory structure and extracts all of the
   * functions located in *.ftl files into a Map.
   */
  public Map<String, String> extractFunctions() {
    Map<String, String> functionMap = new LinkedHashMap<String, String>();
    traverse(getTraversalPath(rootDirectory), rootDirectory, functionMap);
    return functionMap;
  }

  /**
   * Generates the parserImpls.ftl file for the dialect.
   *
   * @param functions The functions to place into the output file
   */
  public void generateParserImpls(Map<String, String> functions) {
    // TODO(AndrewPochapsky): Add generation logic.
  }

  /**
   * Gets the traversal path for the dialect by "subtracting" the root
   * absolute path from the dialect directory absolute path.
   *
   * @param rootDirectoryFile The file for the root parsing directory
   */
  private Queue<String> getTraversalPath(File rootDirectoryFile) {
    Path dialectPath = dialectDirectory.toPath();
    Path rootPath = rootDirectory.toPath();
    dialectPath = dialectPath.subpath(rootPath.getNameCount(),
        dialectPath.getNameCount());
    Queue<String> pathElements = new LinkedList<>();
    dialectPath.forEach(p -> pathElements.add(p.toString()));
    return pathElements;
  }

  /**
   * Traverses the determined path given by the directories queue. Once the
   * queue is empty, the dialect directory has been reached. In that case any
   * *.ftl file should be processed and no further traversal should happen.
   *
   * @param directories The directories to traverse in topdown order
   * @param currentDirectory The current directory the function is processing
   * @param functionMap The map to which the parsing functions will be added to
   */
  private void traverse(
      Queue<String> directories,
      File currentDirectory,
      Map<String, String> functionMap) {
    File[] files = currentDirectory.listFiles();
    // Ensures that files are processed first.
    Arrays.sort(files, fileComparator);
    String nextDirectory = directories.peek();
    for (File f : files) {
      String fileName = f.getName();
      if (fileName.endsWith(".ftl")) {
        try {
          String fileText = new String(Files.readAllBytes(f.toPath()),
              StandardCharsets.UTF_8);
          dialectGenerate.processFile(fileText, functionMap);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (IllegalStateException e) {
          e.printStackTrace();
        }
      } else if (!directories.isEmpty() && fileName.equals(nextDirectory)) {
        // Remove the front element in the queue, the value is referenced above
        // with directories.peek() and is used in the next recursive call to
        // this function.
        directories.poll();
        traverse(directories, f, functionMap);
      }
    }
  }
}
