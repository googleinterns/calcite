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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Objects;
import java.util.TimeZone;

/**
 * Represents a SQL data type specification in a parse tree.
 *
 * <p>A <code>SqlDataTypeSpec</code> is immutable; once created, you cannot
 * change any of the fields.</p>
 *
 * <p>We support the following data type expressions:
 *
 * <ul>
 *   <li>Complex data type expression like:
 *   <blockquote><code>ROW(<br>
 *     foo NUMBER(5, 2) NOT NULL,<br>
 *       rec ROW(b BOOLEAN, i MyUDT NOT NULL))</code></blockquote>
 *   Internally we use {@link SqlRowTypeNameSpec} to specify row data type name.
 *   </li>
 *   <li>Simple data type expression like CHAR, VARCHAR and DOUBLE
 *   with optional precision and scale;
 *   Internally we use {@link SqlBasicTypeNameSpec} to specify basic sql data type name.
 *   </li>
 *   <li>Collection data type expression like:
 *   <blockquote><code>
 *     INT ARRAY;
 *     VARCHAR(20) MULTISET;
 *     INT ARRAY MULTISET;</code></blockquote>
 *   Internally we use {@link SqlCollectionTypeNameSpec} to specify collection data type name.
 *   </li>
 *   <li>User defined data type expression like `My_UDT`;
 *   Internally we use {@link SqlUserDefinedTypeNameSpec} to specify user defined data type name.
 *   </li>
 * </ul>
 */
public class SqlDataTypeSpec extends SqlNode {
  //~ Instance fields --------------------------------------------------------

  private final SqlTypeNameSpec typeNameSpec;
  private final TimeZone timeZone;

  /** Whether data type allows nulls.
   *
   * <p>Nullable is nullable! Null means "not specified". E.g.
   * {@code CAST(x AS INTEGER)} preserves the same nullability as {@code x}.
   */
  private Boolean nullable;

  //Whether data type is uppercase
  private Boolean uppercase;

  //Whether data type is case specific
  private Boolean caseSpecific;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a type specification representing a type.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type
   */
  public SqlDataTypeSpec(
      final SqlTypeNameSpec typeNameSpec,
      SqlParserPos pos) {
    this(typeNameSpec, /*timeZone=*/ null, /*nullable=*/ null, /*uppercase=*/ null,
        /*caseSpecific=*/ null, pos);
  }

  /**
   * Creates a type specification representing a type, with time zone specified.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type
   * @param timeZone     Specified time zone
   */
  public SqlDataTypeSpec(
      final SqlTypeNameSpec typeNameSpec,
      TimeZone timeZone,
      SqlParserPos pos) {
    this(typeNameSpec, timeZone, /*nullable=*/ null, /*uppercase=*/ null,
        /*caseSpecific=*/ null, pos);
  }

  /**
   * Creates a type specification representing a type, with time zone,
   * nullability, uppercase status, caseSpecific status, and base type name
   * specified.
   *
   * @param typeNameSpec The type name can be basic sql type, row type,
   *                     collections type and user defined type
   * @param timeZone     Specified time zone
   * @param nullable     The nullability of the data type
   * @param uppercase    Whether or not the data type is uppercase
   * @param caseSpecific Whether or not the data type is case specific
   */
  public SqlDataTypeSpec(
      SqlTypeNameSpec typeNameSpec,
      TimeZone timeZone,
      Boolean nullable,
      Boolean uppercase,
      Boolean caseSpecific,
      SqlParserPos pos) {
    super(pos);
    this.typeNameSpec = typeNameSpec;
    this.timeZone = timeZone;
    this.nullable = nullable;
    this.uppercase = uppercase;
    this.caseSpecific = caseSpecific;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlNode clone(SqlParserPos pos) {
    return new SqlDataTypeSpec(typeNameSpec, timeZone, pos);
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  public SqlIdentifier getCollectionsTypeName() {
    if (typeNameSpec instanceof SqlCollectionTypeNameSpec) {
      return typeNameSpec.getTypeName();
    }
    return null;
  }

  public SqlIdentifier getTypeName() {
    return typeNameSpec.getTypeName();
  }

  public SqlTypeNameSpec getTypeNameSpec() {
    return typeNameSpec;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public Boolean getNullable() {
    return nullable;
  }

  public Boolean getUppercase() {
    return uppercase;
  }

  public Boolean getCaseSpecific() {
    return caseSpecific;
  }

  /** Returns a copy of this data type specification with a given
    * nullability. */
  public SqlDataTypeSpec withNullable(Boolean nullable) {
    if (Objects.equals(nullable, this.nullable)) {
      return this;
    }
    return new SqlDataTypeSpec(typeNameSpec, timeZone, nullable, uppercase,
        caseSpecific, getParserPosition());
  }

  /** Returns a copy of this data type specification with a given
    * uppercase status. */
  public SqlDataTypeSpec withUppercase(Boolean uppercase) {
    if (Objects.equals(uppercase, this.uppercase)) {
      return this;
    }
    return new SqlDataTypeSpec(typeNameSpec, timeZone, nullable, uppercase,
        caseSpecific, getParserPosition());
  }

  /** Returns a copy of this data type specification with a given
    * caseSpecific status. */
  public SqlDataTypeSpec withCaseSpecific(Boolean caseSpecific) {
    if (Objects.equals(caseSpecific, this.caseSpecific)) {
      return this;
    }
    return new SqlDataTypeSpec(typeNameSpec, timeZone, nullable, uppercase,
        caseSpecific, getParserPosition());
  }

  /**
   * Returns a new SqlDataTypeSpec corresponding to the component type if the
   * type spec is a collections type spec.<br>
   * Collection types are <code>ARRAY</code> and <code>MULTISET</code>.
   */
  public SqlDataTypeSpec getComponentTypeSpec() {
    assert typeNameSpec instanceof SqlCollectionTypeNameSpec;
    SqlTypeNameSpec elementTypeName =
        ((SqlCollectionTypeNameSpec) typeNameSpec).getElementTypeName();
    return new SqlDataTypeSpec(elementTypeName, timeZone, getParserPosition());
  }

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    typeNameSpec.unparse(writer, leftPrec, rightPrec);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDataType(this);
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlDataTypeSpec)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlDataTypeSpec that = (SqlDataTypeSpec) node;
    if (!Objects.equals(this.timeZone, that.timeZone)) {
      return litmus.fail("{} != {}", this, node);
    }
    if (!this.typeNameSpec.equalsDeep(that.typeNameSpec, litmus)) {
      return litmus.fail(null);
    }
    return litmus.succeed();
  }

  /**
   * Converts this type specification to a {@link RelDataType}.
   *
   * <p>Throws an error if the type is not found.
   */
  public RelDataType deriveType(SqlValidator validator) {
    return deriveType(validator, false);
  }

  /**
   * Converts this type specification to a {@link RelDataType}.
   *
   * <p>Throws an error if the type is not found.
   *
   * @param nullable Whether the type is nullable if the type specification
   *                 does not explicitly state
   */
  public RelDataType deriveType(SqlValidator validator, boolean nullable) {
    RelDataType type;
    type = typeNameSpec.deriveType(validator);

    // Fix-up the nullability, default is false.
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    type = fixUpNullability(typeFactory, type, nullable);
    return type;
  }

  //~ Tools ------------------------------------------------------------------

  /**
   * Fix up the nullability of the {@code type}.
   *
   * @param typeFactory Type factory
   * @param type        The type to coerce nullability
   * @param nullable    Default nullability to use if this type specification does not
   *                    specify nullability
   * @return Type with specified nullability or the default(false)
   */
  private RelDataType fixUpNullability(RelDataTypeFactory typeFactory,
      RelDataType type, boolean nullable) {
    if (this.nullable != null) {
      nullable = this.nullable;
    }
    return typeFactory.createTypeWithNullability(type, nullable);
  }
}
