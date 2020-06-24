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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.materialize.MaterializationService;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.Dialct1ParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit tests for the Dialect1 SQL parser.
 */
class Dialect1QuidemTest extends QuidemTest {
  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java Dialect1QuidemTest sql/table.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new Dialect1QuidemTest().test(arg);
    }
  }

  @BeforeEach public void setup() {
    MaterializationService.setThreadLocal();
  }

  /** For {@link QuidemTest#test(String)} parameters. */
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/select.iq";
    return data(first);
  }

  @Override protected Quidem.ConnectionFactory createConnectionFactory() {
    return new QuidemConnectionFactory() {
      @Override public Connection connect(String name, boolean reference)
          throws Exception {
        switch (name) {
        case "scott-dialect1":
          return CalciteAssert.that()
              .with(CalciteAssert.Config.SCOTT)
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  Dialect1ParserImpl.class.getName() + "#FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.DIALECT_1)
              .connect();
        default:
          return super.connect(name, reference);
        }
      }
    };
  }

  @Override protected CommandHandler createCommandHandler() {
    return new Dialect1CommandHandler();
  }

  /** Command that prints the validated parse tree of a SQL statement. */
  static class ExplainValidatedCommand extends AbstractCommand {
    private final ImmutableList<String> lines;
    private final ImmutableList<String> content;
    private final Set<String> productSet;

    ExplainValidatedCommand(List<String> lines, List<String> content,
        Set<String> productSet) {
      this.lines = ImmutableList.copyOf(lines);
      this.content = ImmutableList.copyOf(content);
      this.productSet = ImmutableSet.copyOf(productSet);
    }

    @Override public void execute(Context x, boolean execute) throws Exception {
      if (execute) {
        // use Dialect1 parser
        final SqlParser.ConfigBuilder parserConfig =
            SqlParser.configBuilder()
                .setParserFactory(Dialect1QuidemTest.FACTORY);

        // extract named schema from connection and use it in planner
        final CalciteConnection calciteConnection =
            x.connection().unwrap(CalciteConnection.class);
        final String schemaName = calciteConnection.getSchema();
        final SchemaPlus schema =
            schemaName != null
                ? calciteConnection.getRootSchema().getSubSchema(schemaName)
                : calciteConnection.getRootSchema();
        final Frameworks.ConfigBuilder config =
            Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(parserConfig.build())
                .context(Contexts.of(calciteConnection.config()));

        // parse, validate and un-parse
        final Quidem.SqlCommand sqlCommand = x.previousSqlCommand();
        final Planner planner = Frameworks.getPlanner(config.build());
        final SqlNode node = planner.parse(sqlCommand.sql);
        final SqlNode validateNode = planner.validate(node);
        final SqlWriter sqlWriter = new SqlPrettyWriter();
        validateNode.unparse(sqlWriter, 0, 0);
        x.echo(ImmutableList.of(sqlWriter.toSqlString().getSql()));
      } else {
        x.echo(content);
      }
      x.echo(lines);
    }
  }

  /** Command handler that adds a "!explain-validated-on dialect..." command
   * (see {@link ExplainValidatedCommand}). */
  private static class Dialect1CommandHandler implements CommandHandler {
    @Override public Command parseCommand(List<String> lines,
        List<String> content, String line) {
      final String prefix = "explain-validated-on";
      if (line.startsWith(prefix)) {
        final Pattern pattern =
            Pattern.compile("explain-validated-on( [-_+a-zA-Z0-9]+)*?");
        final Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
          final ImmutableSet.Builder<String> set = ImmutableSet.builder();
          for (int i = 0; i < matcher.groupCount(); i++) {
            set.add(matcher.group(i + 1));
          }
          return new ExplainValidatedCommand(lines, content, set.build());
        }
      }
      return null;
    }
  }
}
