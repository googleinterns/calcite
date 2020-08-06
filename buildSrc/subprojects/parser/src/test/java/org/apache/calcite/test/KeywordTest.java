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

import org.apache.calcite.buildtools.parser.Keyword;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeywordTest {

  @Test public void testHashCodeEquality() {
    Keyword a = new Keyword("foo", "foo", /*filePath=*/ null);
    Keyword b = new Keyword("foo", "foo", /*filePath=*/ null);
    assertEquals(a.hashCode(), b.hashCode());
    Keyword c = new Keyword("foo", "foo", "path");
    Keyword d = new Keyword("foo", "foo", "path");
    assertEquals(c.hashCode(), d.hashCode());
  }

  @Test public void testHashCodeInequality() {
    Keyword a = new Keyword("foo");
    Keyword b = new Keyword("bar");
    assertNotEquals(a.hashCode(), b.hashCode());
    Keyword c = new Keyword("foo", "foo", /*filePath=*/ null);
    Keyword d = new Keyword("foo", "foo", "path");
    assertNotEquals(c.hashCode(), d.hashCode());
  }

  @Test public void testEquality() {
    Keyword a = new Keyword("foo");
    Keyword b = new Keyword("FOO");
    assertEquals(a, a);
    assertEquals(a, b);
    Keyword c = new Keyword("foo", "foo", "path");
    Keyword d = new Keyword("foo", "foo", "path");
    assertEquals(c, d);
    Keyword e = new Keyword("foo", "foo", /*filePath=*/ null);
    Keyword f = new Keyword("foo", "foo", /*filePath=*/ null);
    assertEquals(e, f);
  }

  @Test public void testInequality() {
    Keyword a = new Keyword("foo");
    Keyword b = new Keyword("bar");
    assertNotEquals(a, b);
    assertNotEquals(a, null);
    Keyword c = new Keyword("foo", "foo", /*filePath=*/ null);
    Keyword d = new Keyword("foo", "foo", "path");
    assertNotEquals(c, d);
  }

  @Test public void testKeywordGetsCapitalized() {
    Keyword a = new Keyword("foo");
    assertEquals(a.keyword, "FOO");
  }

  @Test public void testNullKeywordInvalid() {
    assertThrows(NullPointerException.class, () -> new Keyword(null));
  }
}
