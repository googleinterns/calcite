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
import java.nio.file.Paths;
import java.lang.StringBuilder;
import java.lang.IllegalStateException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

/**
 * Contains the logic to extract functions and generate the parserImpls.ftl file
 * for the given dialect. Used by the DialectGenerateTask.kt gradle task.
 */
public class DialectGenerate {

  private static final String typeAndName = "\\w+\\s+\\w+";
  private static final String splitDelims = "(\\s|\n|\"|//|/\\*|\\*/|'|\\}|\\{)";

  // Used to split up a string into tokens by the specified deliminators
  // while also keeping the deliminators as tokens.
  public static final Pattern tokenizerPattern = Pattern.compile("((?<="
      + splitDelims + ")|(?=" + splitDelims + "))");

  // Matches function declarations: <return_type> <name> (<args>) :
  private static final Pattern declarationPattern =
    Pattern.compile("(" + typeAndName + "\\s*\\(\\s*(" + typeAndName + "\\s*(\\,\\s*"
        + typeAndName + "\\s*)*)?\\)\\s*\\:\n?)");
  private static final Pattern namePattern = Pattern.compile("\\w+");

  private static final Comparator<File> fileComparator = new Comparator<File>() {
    @Override
    public int compare(File file1, File file2) {
      return Boolean.compare(file1.isDirectory(), file2.isDirectory());
    }
  };

  private final File dialectDirectory;
  private final File rootDirectory;
  private final String outputFile;

  public DialectGenerate(File dialectDirectory, File rootDirectory, String outputFile) {
    this.dialectDirectory = dialectDirectory;
    this.rootDirectory = rootDirectory;
    this.outputFile = outputFile;
  }

  /**
   * Extracts functions and prints the results for the given dialect.
   */
  public void run() {
    Map<String, String> functions = extractFunctions();
    // TODO(AndrewPochapsky): Remove this once generation logic added.
    for (Map.Entry<String, String> entry : functions.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      System.out.println(key + "=" + value + "\n");
    }
  }

  /**
   * Traverses the parsing directory structure and extracts all of the
   * functions located in *.ftl files into a Map.
   */
  public Map<String, String> extractFunctions() {
    Queue<String> queue = getTraversalPath(rootDirectory);
    Map<String, String> functionMap = new LinkedHashMap<String, String>();
    traverse(queue, rootDirectory, functionMap);
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
    String dialectPath = dialectDirectory.getAbsolutePath();
    String rootPath = rootDirectory.getAbsolutePath();
    int rootIndex = dialectPath.indexOf(rootPath);
    dialectPath = dialectPath.substring(rootIndex + rootPath.length() + 1);
    return new LinkedList(Arrays.asList(dialectPath.split("/")));
  }

  /**
   * Traverses the determined path given by the queue. Once the queue is
   * empty, the dialect directory has been reached. In that case any *.ftl
   * file should be processed and no further traversal should happen.
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
      String extension = "";
      String fileName = f.getName();
      int i = fileName.lastIndexOf('.');
      if (i > 0) {
        extension = fileName.substring(i+1);
      }
      if (f.isFile() && extension.equals("ftl")) {
        try {
          String fileText = new String(Files.readAllBytes(Paths.get(f.getAbsolutePath())),
              StandardCharsets.UTF_8);
          processFile(fileText, functionMap);
        } catch(IOException e) {
          e.printStackTrace();
        }
      } else if (!directories.isEmpty() && fileName.equals(nextDirectory)) {
        // Remove the front element in the queue, the value is used above with
        // directories.peek().
        directories.poll();
        traverse(directories, f, functionMap);
      }
    }
  }

  /**
   * Extracts the functions from the given file into functionMap. Parses
   * functions of the form:
   * <return_type> <name>(<args>) :
   * {
   *     <properties>
   * }
   * {
   *     <body>
   * }
   *
   * @param file The file to process
   * @param functionMap The map to which the parsing functions will be added to
   */
  private void processFile(String fileText, Map<String, String> functionMap) {
    Queue<MatchResult> declarations = new LinkedList();
    Matcher matcher = declarationPattern.matcher(fileText);
    while (matcher.find()) {
      declarations.add(matcher.toMatchResult());
    }
    Queue<String> tokens = new LinkedList(Arrays.asList(tokenizerPattern.split(fileText)));
    int charIndex = 0;
    while (!declarations.isEmpty()) {
      MatchResult declaration = declarations.poll();
      while (charIndex < declaration.start()) {
        charIndex += tokens.poll().length();
      }
      charIndex = processFunction(tokens, functionMap, charIndex,
          declaration.end(),  getFunctionName(declaration.group()));
    }
  }

  /**
   * Parses a function of the form:
   *
   * <return_type> <name>(<args>) :
   * {
   *     <properties>
   * }
   * {
   *     <body>
   * }
   *
   * @param tokens The tokens starting from the function declaration and
   *               ending at EOF
   * @param functionMap The map to which the extracted functions will be added to
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   * @param declarationEnd The char index (of entire file text) at which the
   *                       function declaration ends
   * @param functionName The name of the function being processed
   */
  private int processFunction(
      Queue<String> tokens,
      Map<String, String> functionMap,
      int charIndex,
      int declarationEnd,
      String functionName) {
    StringBuilder functionBuilder = new StringBuilder();
    // Process the declaration:
    while (charIndex < declarationEnd && !tokens.isEmpty()) {
      String token = tokens.poll();
      functionBuilder.append(token);
      charIndex += token.length();
    }
    // Called twice as there are two curly blocks, one for initialization
    // and one for the body.
    charIndex = processCurlyBlock(functionBuilder, tokens, charIndex);
    charIndex = processCurlyBlock(functionBuilder, tokens, charIndex);

    functionMap.put(functionName, functionBuilder.toString());
    return charIndex;
  }

  /**
   * Parses a block of text surrounded by curly braces. Creates an instance of
   * CurlyParser() that is used keep track of whether or not the curly block
   * has been fully parsed or not. The current character index of the file text
   * is updated and returned once parsing is complete.
   *
   * @param functionBuilder The builder unto which the tokens get added to once parsed
   * @param tokens The tokens starting from the function declaration and
   *               ending at EOF
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   */
  private int processCurlyBlock(
      StringBuilder functionBuilder,
      Queue<String> tokens,
      int charIndex) {
    // Remove any preceeding spaces or new lines before the curly block starts.
    charIndex = consumeExtraSpacesAndLines(functionBuilder, tokens, charIndex);
    if (!tokens.peek().equals("{")) {
      System.out.println(tokens);
      throw new IllegalStateException("First token of curly block must be a curly brace.");
    }
    CurlyParser parser = new CurlyParser();
    while (!tokens.isEmpty()) {
      String token = tokens.poll();
      functionBuilder.append(token);
      charIndex += token.length();
      if (parser.parseToken(token)) {
        return charIndex;
      }
    }
    return charIndex;
  }

  /**
   * Consumes tokens while they are either a space or line break. This needs
   * to be called before a curly block begins being parsed to remove any extra
   * spaces or new lines that may preceed it.
   *
   * @param functionBuilder The builder unto which the tokens get added to once parsed
   * @param tokens The tokens starting from charIndex until EOF
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   */
  private int consumeExtraSpacesAndLines(
      StringBuilder functionBuilder,
      Queue<String> tokens,
      int charIndex) {
    while (tokens.peek().equals(" ") || tokens.peek().equals("\n")) {
      String token = tokens.poll();
      charIndex += token.length();
      functionBuilder.append(token);
    }
    return charIndex;
  }

  /**
   * Gets the function name from the declaration.
   *
   * @param functionDeclaration The function declaration of form
   *                            <return_type> <name> (<args>) :
   */
  private String getFunctionName(String functionDeclaration) {
    Matcher m = namePattern.matcher(functionDeclaration);
    // Name is the second match.
    m.find();
    m.find();
    return m.group();
  }

  /**
   * Responsible for parsing a block of text surrounded by curly braces. It is
   * assumed that parsing begins at the start of a curly block. Maintains the
   * state of which structure the parser is currently in to ensure that
   * encountered curly braces are valid.
   */
  public static class CurlyParser {

    /**
     * All of the possible important structures that a token can be inside of.
     * This is tracked to ensure that when a curly brace is encountered,
     * whether it is part of the syntax or not can be determined.
     */
    enum InsideState {

      /**
       * Not inside of anything.
       */
      NONE,

      /**
       * Inside of a string declaration (block of double quotes).
       */
      STRING,

      /**
       * Inside of a character declaration (block of single quotes)
       */
      CHARACTER,

      /**
       * Inside of a single line comment.
       */
      SINGLE_COMMENT,

      /**
       * Inside of a multi line comment.
       */
      MULTI_COMMENT
    }

    // Default state is that the parser is not within any structure.
    private InsideState insideState = InsideState.NONE;

    // Keeps track of the number of open curly braces that have been
    // legally encountered (those that are not within any structure).
    private int curlyCounter = 0;

    /**
     * Parses the given token and updates the state. It is assumed that the
     * tokens are provided as a sequence:
     *
     * for any token1, token2
     * if parseToken(token1) is called and parseToken(token2) is called right after,
     * then token2 comes directly after token1 in the stream of tokens.
     *
     * @param token The token to parse
     * @return Whether or not the curly block has been fully parsed
     */
    public boolean parseToken(String token) {
      updateState(token);
      return curlyCounter == 0;
    }

    /**
     * Determines the updated state based on the token and current state.
     *
     * @param token The token to parse
     */
    private void updateState(String token) {
      if (token.equals("\n")) {
        if (insideState == InsideState.SINGLE_COMMENT) {
          insideState = InsideState.NONE;
        }
      } else if (token.equals("\"")) {
        if (insideState == InsideState.NONE) {
          insideState = InsideState.STRING;
        } else if (insideState == InsideState.STRING) {
          insideState = InsideState.NONE;
        }
      } else if (token.equals("'")) {
        if (insideState == InsideState.NONE) {
          insideState = InsideState.CHARACTER;
        } else if (insideState == InsideState.CHARACTER) {
          insideState = InsideState.NONE;
        }
      } else if (token.equals("//")) {
        if (insideState == InsideState.NONE) {
          insideState = InsideState.SINGLE_COMMENT;
        }
      } else if (token.equals("/*")) {
        if (insideState == InsideState.NONE) {
          insideState = InsideState.MULTI_COMMENT;
        }
      } else if (token.equals("*/")) {
        if (insideState == InsideState.MULTI_COMMENT) {
          insideState = InsideState.NONE;
        }
      } else if (token.equals("{")) {
        if (insideState == InsideState.NONE) {
          curlyCounter++;
        }
      } else if (token.equals("}")) {
        if (insideState == InsideState.NONE) {
          curlyCounter--;
        }
      }
    }
  }
}
