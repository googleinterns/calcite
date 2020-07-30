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
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Traverses the parserImpls tree for the given dialect. Processing is done
 * by <code> DialectGenerate </code>.
 */
public class DialectTraverser {

  private static final Comparator<File> fileComparator = new Comparator<File>() {
    @Override
    public int compare(File file1, File file2) {
      return Boolean.compare(file1.isDirectory(), file2.isDirectory());
    }
  };

  private final File dialectDirectory;
  private final File rootDirectory;
  private final String outputPath;
  private final DialectGenerate dialectGenerate;

  public DialectTraverser(File dialectDirectory, File rootDirectory,
      String outputPath) {
    this.dialectDirectory = dialectDirectory;
    this.rootDirectory = rootDirectory;
    this.outputPath = outputPath;
    this.dialectGenerate = new DialectGenerate();
  }

  /**
   * Extracts functions and token assignments and generates a parserImpls.ftl
   * file containing them at the specified output file.
   */
  public void run() {
    ExtractedData extractedData = extractData();
    generateParserImpls(extractedData);
  }

  /**
   * Traverses the parsing directory structure and extracts all of the
   * functions located in *.ftl files into a Map. This function also compiles
   * the keywords as they are specified throughout the directory structure.
   */
  public ExtractedData extractData() {
    ExtractedData extractedData = new ExtractedData();
    traverse(getTraversalPath(rootDirectory), rootDirectory, extractedData);
    dialectGenerate.unparseReservedKeywords(extractedData);
    dialectGenerate.unparseNonReservedKeywords(extractedData);
    return extractedData;
  }

  /**
   * Generates the parserImpls.ftl file for the dialect. It is assumed that
   * there exists a path src/resources/license.txt at the root parsing directory
   * which was specified in the constructor.
   *
   * @param extractedData The extracted data to write to the output file
   */
  public void generateParserImpls(ExtractedData extractedData) {
    Path outputFilePath = dialectDirectory.toPath().resolve(outputPath);
    Path licensePath = rootDirectory.toPath().resolve(
        Paths.get("src", "resources", "license.txt"));
    StringBuilder content = new StringBuilder();
    try {
      String licenseText = new String(Files.readAllBytes(licensePath),
          StandardCharsets.UTF_8);
      content.append(licenseText);
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (String tokenAssignment : extractedData.tokenAssignments) {
      content.append("\n").append(tokenAssignment).append("\n");
    }
    for (String function : extractedData.functions.values()) {
      content.append("\n").append(function).append("\n");
    }
    File outputFile = outputFilePath.toFile();
    outputFile.getParentFile().mkdirs();
    try {
      Files.write(outputFilePath, content.toString().getBytes());
    } catch (IOException e) {
      e.printStackTrace();
    }
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
    if (dialectPath.equals(rootPath)) {
      return new LinkedList<>();
    }
    dialectPath = dialectPath.subpath(rootPath.getNameCount(),
        dialectPath.getNameCount());
    Queue<String> pathElements = new LinkedList<>();
    dialectPath.forEach(p -> pathElements.add(p.toString()));
    return pathElements;
  }

  /**
   * Traverses the determined path given by the directories queue. Once the
   * queue is empty, the dialect directory has been reached. In that case any
   * *.ftl file should be processed and no further traversal should happen. If
   * a keywords.yaml file is encountered the yaml is converted to a desired
   * format and is processed by {@code dialectGenerate.processKeywords()}.
   *
   * @param directories The directories to traverse in topdown order
   * @param currentDirectory The current directory the function is processing
   * @param functionMap The map to which the parsing functions will be added to
   */
  private void traverse(
      Queue<String> directories,
      File currentDirectory,
      ExtractedData extractedData) {
    File[] files = currentDirectory.listFiles();
    // Ensures that files are processed first.
    Arrays.sort(files, fileComparator);
    String nextDirectory = directories.peek();
    for (File f : files) {
      String fileName = f.getName();
      if (f.isFile()) {
        String fileText = "";
        Path absoluteFilePath = f.toPath();
        try {
          fileText = new String(Files.readAllBytes(absoluteFilePath),
              StandardCharsets.UTF_8);
        } catch (IOException e) {
          e.printStackTrace();
        }
        Path rootPath = rootDirectory.toPath();
        String filePath = absoluteFilePath.subpath(rootPath.getNameCount() - 1,
            absoluteFilePath.getNameCount()).toString();
        // For windows paths change separator to forward slash.
        filePath = filePath.replace('\\', '/');
        if (fileName.endsWith(".ftl")) {
          try {
            dialectGenerate.processFile(fileText, extractedData, filePath);
          } catch (IllegalStateException e) {
            e.printStackTrace();
          }
        } else if (fileName.equals("keywords.yaml")) {
          // This is required to get past the license comment. The part after
          // follows a JSON format is it can be parsed as JSON.
          fileText = fileText.substring(fileText.indexOf("{"));
          JSONObject json = (JSONObject) new JSONTokener(fileText).nextValue();
          JSONArray keywordsJson = json.isNull("keywords")
            ? null
            : json.getJSONArray("keywords");
          JSONArray nonReservedKeywordsJson = json.isNull("nonReservedKeywords")
            ? null
            : json.getJSONArray("nonReservedKeywords");
          Map<Keyword, String> keywords = unparseKeywordsJson(keywordsJson,
              filePath);
          Set<Keyword> nonReservedKeywords = unparseNonReservedKeywordsJson(
              nonReservedKeywordsJson, filePath);
          try {
            dialectGenerate.processKeywords(keywords, nonReservedKeywords,
                extractedData);
          } catch (IllegalStateException e) {
            e.printStackTrace();
          }
        }
      } else if (!directories.isEmpty() && fileName.equals(nextDirectory)) {
        // Remove the front element in the queue, the value is referenced above
        // with directories.peek() and is used in the next recursive call to
        // this function.
        directories.poll();
        traverse(directories, f, extractedData);
      }
    }
  }

  /**
   * Converts the {@code keywordsJson} into a {@code Map<Keyword, String>}.
   * It is assumed that {@code keywordsJson} is either null or of the form:
   *
   * [
   *     {"keyword1": "value1"},
   *     {"keyword2": "value2"}
   *     ...
   * ]
   *
   * Both the key and value are converted to uppercase.
   *
   * @param keywordsJson The keywords json
   * @param filePath The file path these keywords were taken from
   *
   * @return The {@code Map<Keyword, String>} that the json was converted to
   */
  private Map<Keyword, String> unparseKeywordsJson(JSONArray keywordsJson,
      String filePath) {
    Map<Keyword, String> keywords = new LinkedHashMap<Keyword, String>();
    if (keywordsJson == null) {
      return keywords;
    }
    for (Object obj : keywordsJson) {
      JSONObject keywordJson = (JSONObject) obj;
      // There is only one key.
      String keyword = keywordJson.keys().next();
      keywords.put(new Keyword(keyword, filePath),
          keywordJson.getString(keyword).toUpperCase());
    }
    return keywords;
  }

  /**
   * Converts the {@code nonReservedKeywordsJson} into a {@code Set<Keyword>}.
   * It is assumed that {@code nonReservedKeywordsJson} is either null or of
   * the form:
   *
   * [
   *     "keyword1",
   *     "keyword2"
   *     ...
   * ]
   *
   * These values are all converted to uppercase.
   *
   * @param nonReservedKeywordsJson The non reserved keywords json
   * @param filePath The file path these keywords were taken from
   *
   * @return The {@code Map<Keyword, String>} that the json was converted to
   */
  private Set<Keyword> unparseNonReservedKeywordsJson(
      JSONArray nonReservedKeywordsJson, String filePath) {
    Set<Keyword> nonReservedKeywords = new LinkedHashSet<Keyword>();
    if (nonReservedKeywordsJson == null) {
      return nonReservedKeywords;
    }
    for (Object obj : nonReservedKeywordsJson) {
      String keyword = (String) obj;
      nonReservedKeywords.add(new Keyword(keyword, filePath));
    }
    return nonReservedKeywords;
  }
}
