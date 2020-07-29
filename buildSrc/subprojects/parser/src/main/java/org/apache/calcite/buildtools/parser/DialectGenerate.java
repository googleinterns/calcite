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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains the logic to extract functions and generate the parserImpls.ftl file
 * for the given dialect. Used by the DialectGenerateTask.kt gradle task.
 */
public class DialectGenerate {

  // Matches foo<body> where body can be [\w\s<>,]. This allows for easy
  // handling of nested angle brackets and comma separated values.
  private static final String TYPE = "(final\\s+)?[\\w\\.]+\\s*(<\\s*[\\w<>,\\s]+>)?";
  private static final String TYPE_AND_NAME = TYPE + "\\s+\\w+";
  // The \\\\ delim splits by \.
  private static final String SPLIT_DELIMS = "(\\s|\\\\|\n|\"|//|/\\*|\\*/|'|\\}|\\{)";

  // Used to split up a string into tokens by the specified deliminators
  // while also keeping the deliminators as tokens.
  private static final Pattern TOKENIZER_PATTERN = Pattern.compile("((?<="
      + SPLIT_DELIMS + ")|(?=" + SPLIT_DELIMS + "))");

  // Matches function declarations: <return_type> <name> (<args>) :
  private static final Pattern FUNCTION_DECLARATION_PATTERN =
    Pattern.compile("(" + TYPE_AND_NAME + "\\s*\\(\\s*(" + TYPE_AND_NAME + "\\s*(\\,\\s*"
        + TYPE_AND_NAME + "\\s*)*)?\\)\\s*\\:\n?)");
  // Matches the function name within the above function declaration.
  private static final Pattern FUNCTION_NAME_PATTERN = Pattern.compile("(\\w+)\\s*\\(");
  // Matches [<OPT1, OPT2, ...>](TOKEN|SKIP|MORE) :
  private static final Pattern TOKEN_DECLARATION_PATTERN =
    Pattern.compile("((<\\s*\\w+\\s*(\\s*,\\s*\\w+)*\\s*>\\s*)?(TOKEN|SKIP|MORE)\\s*:\n?)");

  public static Queue<String> getTokens(String input) {
    return new LinkedList<String>(Arrays.asList(TOKENIZER_PATTERN.split(input)));
  }

  /**
   * Adds specified keywords and nonReservedKeywords to extractedData. Also
   * ensures that nonReservedKeywords is a subset of the union of keywords
   * and extractedData.keywords.
   *
   * @param keywords The keywords to add
   * @param nonReservedKeywords The non reserved keywords to add
   * @param extractedData The object to which the keywords will be added to
   *
   * @throws IllegalStateException When an an element in nonReservedKeywords is
   *                               not in keywords or extractedData.keywords
   */
  public void processKeywords(Map<Keyword, String> keywords,
      Set<Keyword> nonReservedKeywords,
      ExtractedData extractedData) throws IllegalStateException {
    for (Keyword keyword : nonReservedKeywords) {
      if (!keywords.containsKey(keyword)
            && !extractedData.keywords.containsKey(keyword)) {
        throw new IllegalStateException(keyword.keyword + " is not a keyword.");
      }
    }
    extractedData.keywords.putAll(keywords);
    extractedData.nonReservedKeywords.addAll(nonReservedKeywords);
  }

  /**
   * Adds extractedData.keywords (if nonempty) to extractedData.tokenAssignments
   * with the form:
   * <DEFAULT, DQID, BTID> TOKEN :
   * {
   *    <TOKEN_1: "TOKEN_1_VALUE">
   *   |<TOKEN_2: "TOKEN_2_VALUE">
   *   ...
   * }
   * File annotations are added as single-line comments following each token
   * if the filePath is specified.
   *
   * @param extractedData The object which keeps state of all of the extracted
   *                      data
   */
  public void unparseReservedKeywords(ExtractedData extractedData) {
    if (extractedData.keywords.isEmpty()) {
      return;
    }
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("<DEFAULT, DQID, BTID> TOKEN :\n{\n");
    String tokenTemplate = "<%s : \"%s\">";
    List<String> tokens = new LinkedList<String>();
    for (Map.Entry<Keyword, String> entry : extractedData.keywords.entrySet()) {
      StringBuilder tokenBuilder = new StringBuilder();
      Keyword keyword = entry.getKey();
      tokenBuilder.append(String.format(tokenTemplate, keyword.keyword,
          entry.getValue()));
      if (keyword.filePath == null) {
        tokenBuilder.append(" // No file specified.");
      } else {
        tokenBuilder.append(" // From: ").append(keyword.filePath);
      }
      tokenBuilder.append("\n");
      tokens.add(tokenBuilder.toString());
    }
    stringBuilder.append(String.join("| ", tokens));
    stringBuilder.append("}\n");
    extractedData.tokenAssignments.add(stringBuilder.toString());
  }

  /**
   * Extracts the functions and token assignments from the given file into
   * extractedData. Parses functions of the form:
   * <return_type> <name>(<args>) :
   * {
   *     <properties>
   * }
   * {
   *     <body>
   * }
   *
   * Parses token assignments of the form: [<OPT1, OPT2,...>](TOKEN|SKIP|MORE):
   *                                       { <body> }
   *
   * @param fileText The contents of the file to process
   * @param extractedData The object to which the parsed functions and token assignments
   *             will be added to
   * @param filePath The path of the file being processed
   */
  public void processFile(String fileText, ExtractedData extractedData,
      String filePath) {
    // For windows line endings.
    fileText = fileText.replaceAll("\\r\\n", "\n");
    fileText = fileText.replaceAll("\\r", "\n");
    Queue<MatchResult> functionDeclarations =
      getMatches(FUNCTION_DECLARATION_PATTERN, fileText);
    Queue<MatchResult> tokenAssignmentDeclarations =
      getMatches(TOKEN_DECLARATION_PATTERN, fileText);
    parseDeclarations(functionDeclarations, extractedData, fileText,
        /*isFunctionDeclaration=*/ true, filePath);
    parseDeclarations(tokenAssignmentDeclarations, extractedData, fileText,
        /*isFunctionDeclaration=*/ false, filePath);
  }

  /**
   * Does a single pass of fileText and parses functions or token assignments
   * as they are encountered. The file from which a declaration was
   * extracted from is added as a single-line comment.
   *
   * @param declarations The declarations to parse that are followed by some
   *                     sort of of curly braces
   * @param extractedData Where the extracted functions and token assignments
   *                      are stored
   * @param fileText The text to parse
   * @param isFunctionDeclaration If true, the declarations are functions,
   *                              otherwise the declarations are token
   *                              assignments
   * @param filePath The path of the file being processed
   */
  private void parseDeclarations(Queue<MatchResult> declarations,
      ExtractedData extractedData, String fileText,
      boolean isFunctionDeclaration, String filePath) {
    Queue<String> tokens = getTokens(fileText);
    int charIndex = 0;
    while (!declarations.isEmpty()) {
      MatchResult declaration = declarations.poll();
      while (charIndex < declaration.start()) {
        charIndex += tokens.poll().length();
      }
      StringBuilder stringBuilder = new StringBuilder("\n");
      if (filePath != null) {
        stringBuilder.append("// Extracted from: ").append(filePath).append("\n");
      } else {
        stringBuilder.append("// Extracted file not specified\n");
      }
      if (isFunctionDeclaration) {
        charIndex = processFunction(tokens, extractedData.functions, charIndex,
            declaration.end(), getFunctionName(declaration.group()),
            stringBuilder);
      } else {
        charIndex = processTokenAssignment(tokens,
            extractedData.tokenAssignments, charIndex, declaration.end(),
            stringBuilder);
      }
    }
  }

  private Queue<MatchResult> getMatches(Pattern pattern, String text) {
    Queue<MatchResult> matches = new LinkedList<>();
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()) {
      matches.add(matcher.toMatchResult());
    }
    return matches;
  }

  /**
   * Parses a token assignment of the form:
   *
   * [<OPT1, OPT2,...>](TOKEN|SKIP|MORE):
   * {
   *     <body>
   * }
   *
   * @param tokens The tokens starting from the function declaration and
   *               ending at EOF
   * @param tokenAssignments The list to which the extracted token assignments
   *                         will be added to
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   * @param declarationEnd The char index (of entire file text) at which the
   *                       function declaration ends
   * @param stringBuilder The string builder that tokens will be added to
   */
  public int processTokenAssignment(
      Queue<String> tokens,
      List<String> tokenAssignments,
      int charIndex,
      int declarationEnd,
      StringBuilder stringBuilder) {
    // Process the declaration:
    while (charIndex < declarationEnd && !tokens.isEmpty()) {
      String token = tokens.poll();
      stringBuilder.append(token);
      charIndex += token.length();
    }
    charIndex = processCurlyBlock(stringBuilder, tokens, charIndex);
    tokenAssignments.add(stringBuilder.toString());
    return charIndex;
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
   * @param functionBuilder The string builder that tokens will be added to
   */
  public int processFunction(
      Queue<String> tokens,
      Map<String, String> functionMap,
      int charIndex,
      int declarationEnd,
      String functionName,
      StringBuilder functionBuilder) {
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
      int charIndex) throws IllegalStateException {
    // Remove any preceeding spaces or new lines before the curly block starts.
    charIndex = consumeExtraSpacesAndLines(functionBuilder, tokens, charIndex);
    if (tokens.isEmpty() || !tokens.peek().equals("{")) {
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
    while (!tokens.isEmpty() && (tokens.peek().equals(" ")
        || tokens.peek().equals("\n"))) {
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
  public String getFunctionName(String functionDeclaration) {
    Matcher m = FUNCTION_NAME_PATTERN.matcher(functionDeclaration);
    m.find();
    return m.group(1);
  }
}
