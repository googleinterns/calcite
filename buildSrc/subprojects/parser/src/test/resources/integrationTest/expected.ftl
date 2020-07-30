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

// Extracted from: parserTest/parserImpls.ftl
<DEFAULT> TOKEN :
{

}

// Extracted from: parserTest/intermediate/parserImpls.ftl
SKIP :
{
    < DATE_PART: "DATE_PART" >
|   < DATEADD: "DATEADD" >
|   < DATEDIFF: "DATEDIFF" >
|   < NEGATE: "!" >
|   < TILDE: "~" >
}

// Extracted from: parserTest/intermediate/dialects/testDialect/parserImpls.ftl
MORE : {}

// Auto generated.
<DEFAULT, DQID, BTID> TOKEN :
{
< FOO: "FOO" > // From: parserTest/keywords.yaml
| < BAR: "BAR" > // From: parserTest/keywords.yaml
| < BAZ: "BAZ" > // From: parserTest/keywords.yaml
| < QUX: "QUX1" > // From: parserTest/intermediate/dialects/testDialect/keywords.yaml
| < QUUX: "QUUX1" > // From: parserTest/intermediate/dialects/testDialect/keywords.yaml
}

// Extracted from: parserTest/intermediate/dialects/testDialect/parserImpls.ftl
void foo() :
{
    String x = " '}' ";
}
{
    // overidden by testDialect
}

// Extracted from: parserTest/parserImpls.ftl
void bar  (   )  : {} {}

// Extracted from: parserTest/intermediate/parserImpls.ftl
void baz() : {x}
{
    // overridden by intermediate
}

// Extracted from: parserTest/parserImpls.ftl
void qux(int arg1, int arg2) :
{
    String x = " } "
}
{
    // Below is a }
}

// Extracted from: parserTest/intermediate/parserImpls.ftl
void quux(int arg1,
    int arg2
) :
{
    char x = ' } '
    String y;
}
{
    /* All invalid curly braces:
    }
    // }
    " } "
    ' } '
    */
    <TOKEN> {
        y = " { } } "
    }

    // Not a string: "
}

// Auto generated.
void NonReservedKeyword0of1():
{
}
{
<BAR> // From: parserTest/keywords.yaml
| <BAZ> // From: parserTest/intermediate/keywords.yaml
}

// Auto generated.
String NonReservedKeyWord():
{
}
{
(
NonReservedKeyword0of1()
)
{ return unquotedIdentifier(); }
}
