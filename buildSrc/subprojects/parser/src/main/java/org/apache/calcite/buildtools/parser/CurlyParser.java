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

/**
 * Responsible for parsing a block of text surrounded by curly braces. It is
 * assumed that parsing begins at the start of a curly block. Maintains the
 * state of which structure the parser is currently in to ensure that
 * encountered curly braces are valid.
 */
public class CurlyParser {

  /**
   * All of the possible important structures that a token can be inside of.
   * This is tracked to ensure that when a curly brace is encountered,
   * whether it is part of the syntax or not can be determined.
   */
  enum InsideState {

    /**
     * Not inside of anything except for possibly curly braces.
     */
    NONE,

    /**
     * Inside of a string declaration (block of double quotes).
     */
    STRING,

    /**
     * Inside of a character declaration (block of single quotes).
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

  private String previousToken = "";

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
    } else if (token.equals("\"") && !previousToken.equals("\\")) {
      if (insideState == InsideState.NONE) {
        insideState = InsideState.STRING;
      } else if (insideState == InsideState.STRING) {
        insideState = InsideState.NONE;
      }
    } else if (token.equals("'") && !previousToken.equals("\\")) {
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
    previousToken = token;
  }
}
