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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;


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
   * file containing them at the specified output file. It is assumed that
   * there exists a path src/resources/license.txt at the root parsing directory
   * which was specified in the constructor.
   */
  public void run() {
    String licenseText = getLicenseText();
    ExtractedData extractedData = extractData(licenseText);
    generateParserImpls(extractedData, licenseText);
  }

  /**
   * Traverses the parsing directory structure and extracts all of the
   * functions located in *.ftl files into a Map. This function also compiles
   * the keywords as they are specified throughout the directory structure.
   *
   * @param licenseText The apache license text
   */
  public ExtractedData extractData(String licenseText) {
    ExtractedData extractedData = new ExtractedData();
    traverse(getTraversalPath(), rootDirectory, extractedData, licenseText);
    try {
      dialectGenerate.validateNonReservedKeywords(extractedData);
    } catch (IllegalStateException e) {
      e.printStackTrace();
    }
    dialectGenerate.unparseNonReservedKeywords(extractedData);
    return extractedData;
  }

  /**
   * Reads the contents of the file which contains the apache license.
   *
   * @return The license text if file exists, empty string otherwise
   */
  private String getLicenseText() {
    Path licensePath = rootDirectory.toPath().resolve(
        Paths.get("src", "resources", "license.txt"));
    try {
      return new String(Files.readAllBytes(licensePath),
          StandardCharsets.UTF_8);
    } catch (IOException e ) {
      e.printStackTrace();
    }
    return "";
  }

  /**
   * Generates the parserImpls.ftl file for the dialect.
   *
   * @param extractedData The extracted data to write to the output file
   * @param licenseText The apache license text
   */
  public void generateParserImpls(ExtractedData extractedData,
      String licenseText) {
    Path outputFilePath = dialectDirectory.toPath().resolve(outputPath);
    List<String> specialTokenAssignments = new ArrayList<>();
    specialTokenAssignments.add(dialectGenerate
        .unparseTokenAssignment(extractedData.keywords));
    specialTokenAssignments.add(dialectGenerate
        .unparseTokenAssignment(extractedData.operators));
    specialTokenAssignments.add(dialectGenerate
        .unparseTokenAssignment(extractedData.separators));
    specialTokenAssignments.add(dialectGenerate
        .unparseTokenAssignment(extractedData.identifiers));
    StringBuilder content = new StringBuilder();
    content.append(licenseText);
    for (String tokenAssignment : extractedData.tokenAssignments) {
      content.append("\n").append(tokenAssignment).append("\n");
    }
    for (String specialTokenAssignment : specialTokenAssignments) {
      if (!specialTokenAssignment.isEmpty()) {
        content.append("\n").append(specialTokenAssignment).append("\n");
      }
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
   */
  private Queue<String> getTraversalPath() {
    Path dialectPath = dialectDirectory.toPath();
    Path rootPath = rootDirectory.toPath();
    if (dialectPath.equals(rootPath)) {
      return new ArrayDeque<>();
    }
    dialectPath = dialectPath.subpath(rootPath.getNameCount(),
        dialectPath.getNameCount());
    Queue<String> pathElements = new ArrayDeque<>();
    dialectPath.forEach(p -> pathElements.add(p.toString()));
    return pathElements;
  }

  /**
   * Traverses the determined path given by the directories queue. Once the
   * queue is empty, the dialect directory has been reached. In that case any
   * *.ftl file should be processed and no further traversal should happen. If
   * a *.txt file is found, the contents are processed and added to
   * {@code extractedData}.
   *
   * @param directories The directories to traverse in topdown order
   * @param currentDirectory The current directory the function is processing
   * @param functionMap The map to which the parsing functions will be added to
   * @param licenseText The apache license text
   */
  private void traverse(
      Queue<String> directories,
      File currentDirectory,
      ExtractedData extractedData,
      String licenseText) {
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
        } else if (fileName.endsWith(".txt")) {
          fileText = fileText.substring(licenseText.length());
          String[] lines = fileText.split("\n");
          if (fileName.equals("nonReservedKeywords.txt")) {
            extractedData.nonReservedKeywords
              .addAll(processNonReservedKeywords(lines, filePath));
          } else if (fileName.equals("keywords.txt")) {
            addOrReplaceEntries(extractedData.keywords,
                processKeyValuePairs(lines, filePath));
          } else if (fileName.equals("operators.txt")) {
            addOrReplaceEntries(extractedData.operators,
                processKeyValuePairs(lines, filePath));
          } else if (fileName.equals("separators.txt")) {
            addOrReplaceEntries(extractedData.separators,
                processKeyValuePairs(lines, filePath));
          } else if (fileName.equals("identifiers.txt")) {
            addOrReplaceEntries(extractedData.identifiers,
                processKeyValuePairs(lines, filePath));
          }

        }
      } else if (!directories.isEmpty() && fileName.equals(nextDirectory)) {
        // Remove the front element in the queue, the value is referenced above
        // with directories.peek() and is used in the next recursive call to
        // this function.
        directories.poll();
        traverse(directories, f, extractedData, licenseText);
      }
    }
  }

  /**
   * Adds all of the elements in {@code otherMap} to {@code mainMap}. If a key
   * is already present in {@code mainMap} it is removed before being updated.
   * This is done to ensure that the filePath gets updated if a given keyword
   * has been overriden.
   *
   * @param mainMap The map that is getting entries added to it
   * @param otherMap The map whose entries are being added from
   */
  private void addOrReplaceEntries(Map<Keyword, String> mainMap,
      Map<Keyword, String> otherMap) {
    for (Map.Entry<Keyword, String> entry : otherMap.entrySet()) {
      Keyword key = entry.getKey();
      String value = entry.getValue();
      if (mainMap.containsKey(key)) {
        mainMap.remove(key);
      }
      mainMap.put(key, value);
    }
  }

  /**
   * Parses the {@code lines} array into a {@code Set<Keyword>}. Empty lines
   * are skipped.
   *
   * @param lines The lines to parse
   * @param filePath The file these lines are from
   *
   * @return The parsed lines as a {@code Set<Keyword>}
   */
  private Set<Keyword> processNonReservedKeywords(String[] lines,
      String filePath) {
    Set<Keyword> nonReservedKeywords = new LinkedHashSet<>();
    for (String line : lines) {
      line = line.trim();
      if (!line.isEmpty()) {
        nonReservedKeywords.add(new Keyword(line, filePath));
      }
    }
    return nonReservedKeywords;
  }

  /**
   * Parses the {@code lines} array into a {@code Map<Keyword, String}. It is
   * assumed that each line contains a key-value pair separated by a colon.
   * Empty lines are skipped.
   *
   * @param lines The lines to parse
   * @param filepath The file these lines came from
   */
  private Map<Keyword, String> processKeyValuePairs(String[] lines,
      String filePath) {
    Map<Keyword, String> map = new LinkedHashMap<>();
    for (String line : lines) {
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      int colonIndex = line.indexOf(":");
      String key = line.substring(0, colonIndex);
      String value = line.substring(colonIndex + 1);
      map.put(new Keyword(key.trim(), filePath), value.trim());
    }
    return map;
  }
}
