package org.apache.calcite.sql;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import static org.apache.calcite.util.Static.RESOURCE;

public class SqlNullTreatmentModifier extends SqlNode {
  public final SqlKind kind;

  public SqlNullTreatmentModifier(SqlParserPos pos, SqlKind kind) {
    super(pos);
    this.kind = Objects.requireNonNull(kind);
    Preconditions.checkArgument(kind == SqlKind.RESPECT_NULLS
        || kind == SqlKind.IGNORE_NULLS);
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return null;
  }

  @Override public void validate(
      SqlValidator validator,
      SqlValidatorScope scope) {
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
    return false;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec,int rightPrec) {
    if (kind == SqlKind.RESPECT_NULLS) {
      writer.keyword("RESPECT NULLS");
    } else {
      writer.keyword("IGNORE NULLS");
    }
  }
}

