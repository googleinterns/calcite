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

import java.util.Objects;

/**
 * Simple container class for a keyword.
 */
public class Keyword {
  public final String keyword;
  public final String value;
  public final String filePath;

  public Keyword(String keyword) {
    this(keyword, keyword, /*filePath=*/ null);
  }

  public Keyword(String keyword, String value) {
    this(keyword, value, /*filePath=*/ null);
  }

  /**
   * Creates a {@code Keyword}.
   *
   * @param keyword The name of the keyword
   * @param value The value of the keyword
   * @param filePath The file where this keyword was taken from
   */
  public Keyword(String keyword, String value, String filePath) {
    this.keyword = Objects.requireNonNull(keyword.toUpperCase());
    this.value = Objects.requireNonNull(value);
    this.filePath = filePath;
  }

  @Override public int hashCode() {
    int hashCode = keyword.hashCode() * value.hashCode();
    if (filePath != null) {
      hashCode *= filePath.hashCode();
    }
    return hashCode;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof Keyword)) {
      return false;
    }
    Keyword other = (Keyword) obj;
    if (!this.keyword.equals(other.keyword) && !this.value.equals(other.value)) {
      return false;
    }
    if (this.filePath == null && other.filePath == null) {
      return true;
    }
    return this.filePath != null && other.filePath != null
        && this.filePath.equals(other.filePath);
  }
}
