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
import org.apache.calcite.runtime.Resources;
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

import au.com.bytecode.opencsv.CSVReader;

import com.google.gson.stream.JsonWriter;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Processes a CSV file containing queries and will output a JSON file containing the results of
 * failing queries.
 */
public class FindDialectParsingErrors {

  private final String inputPath;
  private final String outputPath;
  private final Dialect dialect;
  private boolean groupByErrors;
  private int numSampleQueries;
  private final List<MessageFormat> errorFormats;

  /**
   *  Creates a new instance of {@code FindDialectParsingErrors}. Also populates the errorFormats list with
   *  a MessageFormat object for each custom defined error message in CalciteResource.
   *
   * @param inputPath Path to the input CSV file containing queries
   * @param outputPath Path to the output JSON file containing results of failing queries
   * @param dialect Specifies which dialectic parser to use for processing queries
   * @param groupByErrors Specifies if the output format should group failing queries by error
   *                      message type
   * @param numSampleQueries The max number of sample queries to show for an error type when
   *                         grouping queries by error message type
   */
  public FindDialectParsingErrors(String inputPath, String outputPath, Dialect dialect,
      boolean groupByErrors, int numSampleQueries) {
    this.inputPath = inputPath;
    this.outputPath = outputPath;
    this.dialect = dialect;
    this.groupByErrors = groupByErrors;
    this.numSampleQueries = numSampleQueries;
    errorFormats = new ArrayList<>();
    Method[] methods = CalciteResource.class.getMethods();
    for (Method m : methods) {
      errorFormats.add(
        new MessageFormat(m.getAnnotationsByType(Resources.BaseMessage.class)[0].value())
      );
    }
  }

  /**
   * Runs one of the processing methods and will output a JSON file containing the results of
   * failing queries. This will run findErrorGroups if groupByErrors is true. Otherwise,
   * findFullErrors will run.
   *
   * @throws IOException If it fails to read the input file
   */
  public void run() throws IOException {
    CSVReader reader = new CSVReader(new InputStreamReader(new FileInputStream(inputPath)), ',',
      '"', 1);
    List<String[]> rows = reader.readAll();
    reader.close();
    List<String> queries = rows.parallelStream()
      .map(row -> sanitize(row[0]))
      .collect(Collectors.toList());
    if (groupByErrors) {
      findErrorGroups(queries);
    } else {
      findFullErrors(queries);
    }
  }

  /**
   * This is the default processing method. It parallel processes the list of queries and finds the
   * full error for any failing query.
   *
   * @param queries A list of sanitized queries
   * @throws IOException If it fails to write to the output file
   */
  private void findFullErrors(List<String> queries) throws IOException {
    SqlParser.Config config = SqlParser.configBuilder()
      .setParserFactory(dialect.getDialectFactory())
      .build();
    Map<String, FullError> errors = queries.parallelStream()
      .map(query -> {
        try {
          SqlParser.create(query, config).parseStmt();
          return null;
        } catch (SqlParseException e) {
          return new FullError(query, e.getMessage(), 1);
        }
      })
      .filter(Objects::nonNull)
      .collect(
        Collectors.toConcurrentMap(e -> e.query, e -> e, (e1, e2) -> {
          e1.count += e2.count;
          return e1;
        }
      ));
    int numFailed = 0;
    for (Map.Entry<String, FullError> entry : errors.entrySet()) {
      numFailed += entry.getValue().count;
    }
    outputFullErrorResults(errors, queries.size() - numFailed, numFailed);
  }

  /**
   * This method is called when the command line option --groupByErrors is specified. It will group
   * failing queries together based on the error message type, as well as keep a set of sample
   * queries for its error type. This method parallel processes the queries list.
   *
   * @param queries A list of sanitized queries
   * @throws IOException If it fails to write to the output file
   */
  private void findErrorGroups(List<String> queries) throws IOException {
    SqlParser.Config config = SqlParser.configBuilder()
      .setParserFactory(dialect.getDialectFactory())
      .build();
    Map<String, ErrorType> errors = queries.parallelStream()
      .map(query -> {
        try {
          SqlParser.create(query, config).parseStmt();
          return null;
        } catch (SqlParseException ex) {
          String message = ex.getMessage();
          List<String> sampleQueries = new ArrayList<>();
          sampleQueries.add(query);
          return new ErrorType(1, message,
            processErrorMessage(message), sampleQueries);
        }
      })
      .filter(Objects::nonNull)
      .collect(
        Collectors.toConcurrentMap(e -> e.type, e -> e, (e1, e2) -> {
          ErrorType errorCountValue = new ErrorType();
          errorCountValue.count = e1.count + e2.count;
          errorCountValue.fullError = e1.fullError;
          errorCountValue.type = e1.type;
          errorCountValue.sampleQueries.addAll(e1.sampleQueries);
          for (int i = 0; i < e2.sampleQueries.size()
              && errorCountValue.sampleQueries.size() < numSampleQueries; i++) {
            errorCountValue.sampleQueries.add(e2.sampleQueries.get(i));
          }
          return errorCountValue;
      }));
    int numFailed = 0;
    for (Map.Entry<String, ErrorType> entry : errors.entrySet()) {
      numFailed += entry.getValue().count;
    }
    outputErrorGroupResults(errors, queries.size() - numFailed, numFailed);
  }

  /**
   * Outputs the processed results into a JSON file containing a list JSON objects where each object
   * contains the query and the error message. This method also outputs the number of successful
   * queries and the number of failed queries.
   *
   * @param errors A Map of queries and their full errors
   * @param numPassed The number of queries that successfully parsed
   * @param numFailed The number of queries that failed to parse
   * @throws IOException If it fails to create a FileWriter at the the output path
   */
  private void outputFullErrorResults(Map<String, FullError> errors, int numPassed, int numFailed)
      throws IOException {
    JsonWriter writer = new JsonWriter(new FileWriter(outputPath));
    writer.setIndent("  ");
    writer.beginObject();
    writer.name("numPassed").value(numPassed);
    writer.name("numFailed").value(numFailed);
    writer.name("errors");
    writer.beginArray();
    for (Map.Entry<String, FullError> entry : errors.entrySet()) {
      writer.beginObject();
      writer.name("query").value(entry.getKey());
      writer.name("error").value(entry.getValue().error);
      writer.endObject();
    }
    writer.endArray();
    writer.endObject();
    writer.close();
  }

  /**
   * Outputs the processed results into a JSON file containing a list, sorted by count in
   * descending order, of error JSON objects where each object contains the values inside the
   * ErrorCountValue object. This method also outputs the number of successful queries and the
   * number of failed queries.
   *
   * @param errors A Map of error type groups
   * @param numPassed The number of queries that successfully parsed
   * @param numFailed The number of queries that failed to parse
   * @throws IOException If it fails to create a FileWriter at the output path
   */
  private void outputErrorGroupResults(Map<String, ErrorType> errors, int numPassed,
      int numFailed) throws IOException {
    JsonWriter writer = new JsonWriter(new FileWriter(outputPath));
    writer.setIndent("  ");
    writer.beginObject();
    writer.name("numPassed").value(numPassed);
    writer.name("numFailed").value(numFailed);
    writer.name("errors");
    writer.beginArray();
    List<Map.Entry<String, ErrorType>> sortedEntries = new ArrayList<>(errors.entrySet());
    sortedEntries.sort(Comparator.comparing(e -> -e.getValue().count));
    for (Map.Entry<String, ErrorType> e : sortedEntries) {
      writeErrorGroupJsonObject(writer, e);
    }
    writer.endArray();
    writer.endObject();
    writer.close();
  }

  /**
   * Writes a single JSON object containing the values inside the ErrorCountValue object for the
   * provided entry.
   *
   * @param writer The JsonWriter setup for the output path
   * @param entry The entry to write as a JSON Object
   * @throws IOException If it fails to write to the output file
   */
  private void writeErrorGroupJsonObject(JsonWriter writer,
      Map.Entry<String, ErrorType> entry) throws IOException {
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

  /**
   * Parses the error message and returns which error type it is. If the error message contains
   * "Encountered", it will use the tokens occurring after "Encountered" as the error type.
   * Otherwise, it will use one of the custom defined error messages inside CalciteResource.
   *
   * @param message The error message to process
   * @return The error message type
   */
  private String processErrorMessage(String message) {
    String encounteredToken = "Encountered \"";
    int start = message.indexOf(encounteredToken);
    if (start != -1) {
      int end = message.indexOf("\"", start + encounteredToken.length());
      return message.substring(start, end + 1);
    }
    for (MessageFormat errorFormat : errorFormats) {
      try {
        errorFormat.parse(message);
        return errorFormat.toPattern();
      } catch (ParseException ignored) {
      }
    }
    return message;
  }

  /**
   * Sanitizes the provided query. It will remove a semicolon from the end and replace some unicode
   * characters that cannot be parsed by Calcite with an ASCII equivalent character.
   *
   * @param query The query to sanitize
   * @return The sanitized query
   */
  private String sanitize(String query) {
    HashMap<Character, Character> replaceChars = new HashMap<>();
    replaceChars.put('\u00A0', ' ');
    replaceChars.put('\u2013', '-');
    replaceChars.put('\u2018', '\'');
    replaceChars.put('\u2019', '\'');
    replaceChars.put('\u201c', '\"');
    replaceChars.put('\u201d', '\"');
    replaceChars.put('\u2028', ' ');
    for (Map.Entry<Character, Character> entry : replaceChars.entrySet()) {
      query = query.replace(entry.getKey(), entry.getValue());
    }
    query = query.trim();

    if (query.endsWith(";")) {
      query = query.substring(0, query.length() - 1);
    }
    return query;
  }

  class ErrorType {
    Integer count;
    String fullError;
    String type;
    List<String> sampleQueries;

    public ErrorType() {
      count = 0;
      sampleQueries = new ArrayList<>();
    }

    public ErrorType(Integer count, String fullError, String type, List<String> sampleQueries) {
      this.count = count;
      this.fullError = fullError;
      this.type = type;
      this.sampleQueries = sampleQueries;
    }
  }

  class FullError {
    String query;
    String error;
    int count;

    public FullError(String query, String error, int count) {
      this.query = query;
      this.error = error;
      this.count = count;
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
