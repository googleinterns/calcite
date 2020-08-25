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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code CREATE PROCEDURE} statement.
 */
public class SqlCreateProcedure extends SqlCreate {
  public final SqlIdentifier procedureName;
  public final List<SqlCreateProcedureParameter> parameters;
  public final CreateProcedureDataAccess access;
  public final SqlLiteral numResultSets;
  public final CreateProcedureSecurity security;
  public final SqlNode statement;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE PROCEDURE", SqlKind.CREATE_PROCEDURE);

  /**
   * Creates an instance of {@code SqlCreateProcedure}.
   *
   * @param pos Parser position
   * @param createSpecifier Enum to distinguish between CREATE and REPLACE
   *                        statements
   * @param procedureName Name of procedure
   * @param parameters Input and output parameters
   * @param access Level of data access
   * @param numResultSets Number of dynamic result sets, may be null
   * @param security SQL security level of procedure
   * @param statement The statement ran when the procedure is called
   */
  public SqlCreateProcedure(SqlParserPos pos, SqlCreateSpecifier createSpecifier,
      SqlIdentifier procedureName,
      List<SqlCreateProcedureParameter> parameters,
      CreateProcedureDataAccess access, SqlLiteral numResultSets,
      CreateProcedureSecurity security, SqlNode statement) {
    super(OPERATOR, pos, createSpecifier, false);
    this.procedureName = procedureName;
    this.parameters = parameters;
    this.access = access;
    this.numResultSets = numResultSets;
    this.security = security;
    this.statement = statement;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(procedureName, numResultSets, statement);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getCreateSpecifier().toString());
    writer.keyword("PROCEDURE");
    procedureName.unparse(writer, 0, 0);
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlCreateProcedureParameter p : parameters) {
      writer.sep(",");
      p.unparse(writer, 0, 0);
    }
    writer.endList(frame);
    switch (access) {
    case CONTAINS_SQL:
      writer.keyword("CONTAINS SQL");
      break;
    case MODIFIES_SQL_DATA:
      writer.keyword("MODIFIES SQL DATA");
      break;
    case READS_SQL_DATA:
      writer.keyword("READS SQL DATA");
      break;
    default:
      break;
    }
    if (numResultSets != null) {
      writer.keyword("DYNAMIC RESULT SETS");
      numResultSets.unparse(writer, 0, 0);
    }
    if (security != CreateProcedureSecurity.UNSPECIFIED) {
      writer.keyword("SQL SECURITY");
      writer.keyword(security.toString());
    }
    writer.newlineAndIndent();
    statement.unparse(writer, 0, 0);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    validator.addProcedureToSchema(this);
    validator.validateScriptingStatement(statement, scope);
  }

  public enum CreateProcedureSecurity {
    /**
     * Assigns privileges of the creator of a procedure.
     */
    CREATOR,

    /**
     * Assigns privileges of the definer of a procedure.
     */
    DEFINER,

    /**
     * Assigns privileges of the invoker of a procedure.
     */
    INVOKER,

    /**
     * Assigns privileges of the owner of a procedure.
     */
    OWNER,

    /**
     * No security privilege is specified.
     */
    UNSPECIFIED,
  }

  public enum CreateProcedureDataAccess {
    /**
     * The procedure can only execute SQL control statements.
     */
    CONTAINS_SQL,

    /**
     * The procedure can execute all SQL statements.
     */
    MODIFIES_SQL_DATA,

    /**
     * The procedure can only execute SQL statements that read SQL data.
     */
    READS_SQL_DATA,

    /**
     * No data access is specified.
     */
    UNSPECIFIED,
  }
}
