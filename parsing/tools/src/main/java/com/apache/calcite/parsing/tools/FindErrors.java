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
package com.apache.calcite.parsing.tools;

import org.apache.calcite.runtime.CalciteResource;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.bigquery.BigQueryParserImpl;
import org.apache.calcite.sql.parser.defaultdialect.DefaultDialectParserImpl;
import org.apache.calcite.sql.parser.dialect1.Dialect1ParserImpl;
import org.apache.calcite.sql.parser.hive.HiveParserImpl;
import org.apache.calcite.sql.parser.mysql.MySQLParserImpl;
import org.apache.calcite.sql.parser.postgresql.PostgreSQLParserImpl;
import org.apache.calcite.sql.parser.redshift.RedshiftParserImpl;
import org.apache.calcite.util.Static;

import com.google.gson.stream.JsonWriter;

import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FindErrors {

  private final String inputPath;
  private final String outputPath;
  private final Dialect dialect;
  private boolean groupByErrors = false;
  private int numSampleQueries = 5;

  public void run() throws IOException { // TODO Handle error
    CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(inputPath)), ',',
        '"', 1);
    List<String[]> rows = reader.readAll();
    reader.close();
    List<String> queries = new ArrayList<>();
    for (String[] row : rows) {
      queries.add(sanitize(row[0]));
    }
    if (groupByErrors) {
      findErrorGroups(queries);
    }
  }

  public FindErrors(String inputPath, String outputPath, Dialect dialect,
      boolean groupByErrors, int numSampleQueries) {
    this.inputPath = inputPath;
    this.outputPath = outputPath;
    this.dialect = dialect;
    this.groupByErrors = groupByErrors;
    this.numSampleQueries = numSampleQueries;
  }

  private void findErrorGroups(List<String> queries) throws IOException {
    SqlParser.Config config = SqlParser.configBuilder()
      .setParserFactory(dialect.getDialectFactory())
      .build();
    Map<String, ErrorCountValue> errors;
    errors = queries.stream()
      .map(query -> {
        try {
          SqlParser.create(query, config).parseStmt();
          return null;
        } catch (SqlParseException ex) {
          String message = ex.getMessage();
          List<String> sampleQueries = new ArrayList<>();
          sampleQueries.add(query);
          return new ErrorCountValue(1, message,
              processErrorMessage(message), sampleQueries);
        }
      })
      .filter(Objects::nonNull)
      .collect(
          Collectors.toMap(e -> e.group, e -> e, (e1, e2) -> {
        ErrorCountValue errorCountValue = new ErrorCountValue();
        errorCountValue.count = e1.count + e2.count;
        errorCountValue.fullError = e1.fullError;
        errorCountValue.group = e1.group;
        errorCountValue.sampleQueries.addAll(e1.sampleQueries);
        for (int i = 0; i < e2.sampleQueries.size()
            && errorCountValue.sampleQueries.size() < numSampleQueries; i++) {
          errorCountValue.sampleQueries.add(e2.sampleQueries.get(i));
        }
        return errorCountValue;
      }));
    int numFailed = 0;
    for (Map.Entry<String, ErrorCountValue> entry : errors.entrySet()) {
      numFailed += entry.getValue().count;
    }
    outputErrorGroupResults(errors, queries.size() - numFailed, numFailed);
  }

  private void outputErrorGroupResults(Map<String, ErrorCountValue> errors, int numPassed,
      int numFailed) throws IOException {
    JsonWriter writer = new JsonWriter(new FileWriter(outputPath));
    writer.setIndent("  ");
    writer.beginObject();
    writer.name("numPassed").value(numPassed);
    writer.name("numFailed").value(numFailed);
    writer.name("errors");
    writer.beginArray();
    errors.entrySet().stream()
      .sorted(Comparator.comparing(e -> -e.getValue().count))
      .forEach(e -> {
        try {
          writeErrorGroupJsonObject(writer, e);
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      });
    writer.endArray();
    writer.endObject();
    writer.close();
  }

  private void writeErrorGroupJsonObject(JsonWriter writer,
      Map.Entry<String, ErrorCountValue> entry) throws IOException {
    writer.beginObject();
    writer.name("errorMessageType").value(entry.getKey());
    writer.name("count").value(entry.getValue().count);
    writer.name("sampleQueries");
    writer.beginArray();
    for (String query : entry.getValue().sampleQueries) {
      writer.value(query);
    }
    writer.endArray();
    writer.name("fullError").value(entry.getValue().fullError);
    writer.endObject();
  }

  private String processErrorMessage(String message) {
    String encounteredToken = "Encountered \"";
    int start = message.indexOf(encounteredToken);
    if (start != -1) {
      int end = message.indexOf("\"", start + encounteredToken.length());
      return message.substring(start, end + 1);
    }
    return null;
  }

  private String sanitize(String query) {
//    query = query.replace('\u00A0',' ').trim();
    if (query.endsWith(";")) {
      query = query.substring(0, query.length() - 1);
    }
    return query;
  }

  class ErrorCountValue {
    Integer count;
    String fullError;
    String group;
    List<String> sampleQueries;

    public ErrorCountValue() {
      count = 0;
      sampleQueries = new ArrayList<>();
    }

    public ErrorCountValue(Integer count, String fullError, String group, List<String> sampleQueries) {
      this.count = count;
      this.fullError = fullError;
      this.group = group;
      this.sampleQueries = sampleQueries;
    }
  }

  enum Dialect {
    BIGQUERY {
      @Override public SqlParserImplFactory getDialectFactory() {
        return BigQueryParserImpl.FACTORY;
      }
    }, DEFAULTDIALECT {
      @Override public SqlParserImplFactory getDialectFactory() {
        return DefaultDialectParserImpl.FACTORY;
      }
    }, DIALECT1 {
      @Override public SqlParserImplFactory getDialectFactory() {
        return Dialect1ParserImpl.FACTORY;
      }
    }, HIVE {
      @Override public SqlParserImplFactory getDialectFactory() {
        return HiveParserImpl.FACTORY;
      }
    }, MYSQL {
      @Override public SqlParserImplFactory getDialectFactory() {
        return MySQLParserImpl.FACTORY;
      }
    }, POSTGRESQL {
      @Override public SqlParserImplFactory getDialectFactory() {
        return PostgreSQLParserImpl.FACTORY;
      }
    }, REDSHIFT {
      @Override public SqlParserImplFactory getDialectFactory() {
        return RedshiftParserImpl.FACTORY;
      }
    };
    public abstract SqlParserImplFactory getDialectFactory();
  }

}
