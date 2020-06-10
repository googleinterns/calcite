<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

JoinType LeftSemiJoin() :
{
}
{
    <LEFT> <SEMI> <JOIN> { return JoinType.LEFT_SEMI_JOIN; }
}

SqlNode DateFunctionCall() :
{
    final SqlFunctionCategory funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION;
    final SqlIdentifier qualifiedName;
    final Span s;
    final SqlLiteral quantifier;
    final List<? extends SqlNode> args;
}
{
    <DATE> {
        s = span();
        qualifiedName = new SqlIdentifier(unquotedIdentifier(), getPos());
    }
    args = FunctionParameterList(ExprContext.ACCEPT_SUB_QUERY) {
        quantifier = (SqlLiteral) args.get(0);
        args.remove(0);
        return createCall(qualifiedName, s.end(this), funcType, quantifier, args);
    }
}

SqlNode DateaddFunctionCall() :
{
    final SqlFunctionCategory funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION;
    final Span s;
    final SqlIdentifier qualifiedName;
    final TimeUnit unit;
    final List<SqlNode> args;
    SqlNode e;
}
{
    ( <DATEADD> | <DATEDIFF> | <DATE_PART> ) {
        s = span();
        qualifiedName = new SqlIdentifier(unquotedIdentifier(), getPos());
    }
    <LPAREN> unit = TimeUnit() {
        args = startList(new SqlIntervalQualifier(unit, null, getPos()));
    }
    (
        <COMMA> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args.add(e);
        }
    )*
    <RPAREN> {
        return createCall(qualifiedName, s.end(this), funcType, null, args);
    }
}

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

SetType SetTypeOpt() :
{
}
{
    <MULTISET> { return SetType.MULTISET; }
|
    <SET> { return SetType.SET; }
|
    { return SetType.UNSPECIFIED; }
}

Volatility VolatilityOpt() :
{
}
{
    <VOLATILE> { return Volatility.VOLATILE; }
|
    <TEMP> { return Volatility.TEMP; }
|
    { return Volatility.UNSPECIFIED; }
}

OnCommitType OnCommitTypeOpt() :
{
    OnCommitType onCommitType;
}
{
    (
        <ON> <COMMIT>
        (
            <PRESERVE> { onCommitType = OnCommitType.PRESERVE; }
        |
            <RELEASE> { onCommitType = OnCommitType.RELEASE; }
        )
        <ROWS>
    |
        { onCommitType = OnCommitType.UNSPECIFIED; }
    )
    { return onCommitType; }
}

SqlNodeList ExtendColumnList() :
{
    final Span s;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ColumnWithType(list)
    (
        <COMMA> ColumnWithType(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

boolean IsNullable() :
{

}
{
    (
        <NOT> <NULL> {
            return false;
        }
    |
        <NULL> {
            return true;
        }
    )
}

boolean IsCaseSpecific() :
{

}
{
    (
        <NOT> <CASESPECIFIC> {
            return false;
        }
    |
        <CASESPECIFIC> {
            return true;
        }
    )
}

boolean IsUpperCase() :
{

}
{
    (
        <NOT> <UPPERCASE> {
            return false;
        }
    |
        <UPPERCASE> {
            return true;
        }
    )
}

void ColumnWithType(List<SqlNode> list) :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    Boolean uppercase = null;
    Boolean caseSpecific = null;
    final Span s = Span.of();
}
{
    id = CompoundIdentifier()
    type = DataType()
    // This acts as a loop to check which optional parameters have been specified.
    (
        nullable = IsNullable()
        {
            type = type.withNullable(nullable);
        }
    |
        uppercase = IsUpperCase() {
            type = type.withUppercase(uppercase);
        }
    |
        caseSpecific = IsCaseSpecific() {
            type = type.withCaseSpecific(caseSpecific);
        }
    )*
    {
        list.add(SqlDdlNodes.column(s.add(id).end(this), id, type, null, null));
    }
}

SqlCreateAttribute CreateTableAttributeFallback() :
{
    boolean no = false;
    boolean protection = false;
}
{
    [ <NO>  { no = true; } ]
    <FALLBACK>
    [ <PROTECTION> { protection = true; } ]
    { return new SqlCreateAttributeFallback(no, protection, getPos()); }
}

SqlCreateAttribute CreateTableAttributeJournalTable() :
{
    final SqlIdentifier id;
}
{
    <WITH> <JOURNAL> <TABLE> <EQ> id = CompoundIdentifier()
    { return new SqlCreateAttributeJournalTable(id, getPos()); }
}

// FREESPACE attribute can take in decimals but should be truncated to an integer.
SqlCreateAttribute CreateTableAttributeFreeSpace() :
{
    SqlLiteral tempNumeric;
    int freeSpaceValue;
    boolean percent = false;
}
{
    <FREESPACE> <EQ> tempNumeric = UnsignedNumericLiteral() {
        freeSpaceValue = tempNumeric.getValueAs(Integer.class);
        if (freeSpaceValue < 0 || freeSpaceValue > 75) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.numberLiteralOutOfRange(String.valueOf(freeSpaceValue)));
        }
    }
    [ <PERCENT> { percent = true; } ]
    { return new SqlCreateAttributeFreeSpace(freeSpaceValue, percent, getPos()); }
}

SqlCreateAttribute CreateTableAttributeIsolatedLoading() :
{
    boolean nonLoadIsolated = false;
    boolean concurrent = false;
    OperationLevel operationLevel = null;
}
{
    <WITH>
    [ <NO> { nonLoadIsolated = true; } ]
    [ <CONCURRENT> { concurrent = true; } ]
    <ISOLATED> <LOADING>
    [
        <FOR>
        (
            <ALL> { operationLevel = OperationLevel.ALL; }
        |
            <INSERT> { operationLevel = OperationLevel.INSERT; }
        |
            <NONE> { operationLevel = OperationLevel.NONE; }
        )
    ]
    { return new SqlCreateAttributeIsolatedLoading(nonLoadIsolated, concurrent, operationLevel, getPos()); }
}

SqlCreateAttribute CreateTableAttributeJournal() :
{
  JournalType journalType;
  JournalModifier journalModifier;
}
{
    (
        (
            <LOCAL> { journalModifier = JournalModifier.LOCAL; }
        |
            <NOT> <LOCAL> { journalModifier = JournalModifier.NOT_LOCAL; }
        )
        <AFTER> <JOURNAL> { journalType = JournalType.AFTER; }
    |
        (
            <NO> { journalModifier = JournalModifier.NO; }
        |
            <DUAL> { journalModifier = JournalModifier.DUAL; }
        |
            { journalModifier = JournalModifier.UNSPECIFIED; }
        )
        (
            <BEFORE> { journalType = JournalType.BEFORE; }
        |
            <AFTER> { journalType = JournalType.AFTER; }
        |
            { journalType = JournalType.UNSPECIFIED; }
        )
        <JOURNAL>
    )
    { return new SqlCreateAttributeJournal(journalType, journalModifier, getPos()); }
}

SqlCreateAttribute CreateTableAttributeDataBlockSize() :
{
    DataBlockModifier modifier = null;
    DataBlockUnitSize unitSize;
    SqlLiteral dataBlockSize = null;
}
{
    (
        (
            ( <MINIMUM> | <MIN> ) { modifier = DataBlockModifier.MINIMUM; }
        |
            ( <MAXIMUM> | <MAX> ) { modifier = DataBlockModifier.MAXIMUM; }
        |
            <DEFAULT_> { modifier = DataBlockModifier.DEFAULT; }
        )
        <DATABLOCKSIZE> { unitSize = DataBlockUnitSize.BYTES; }
    |
        <DATABLOCKSIZE> <EQ> dataBlockSize = UnsignedNumericLiteral()
        (
            ( <KILOBYTES> | <KBYTES> ) { unitSize = DataBlockUnitSize.KILOBYTES; }
        |
            [ <BYTES> ] { unitSize = DataBlockUnitSize.BYTES; }
        )
    )
    { return new SqlCreateAttributeDataBlockSize(modifier, unitSize, dataBlockSize, getPos()); }
}

SqlCreateAttribute CreateTableAttributeMergeBlockRatio() :
{
    MergeBlockRatioModifier modifier = MergeBlockRatioModifier.UNSPECIFIED;
    int ratio = 1;
    boolean percent = false;
}
{
    (
        (
            <DEFAULT_> { modifier = MergeBlockRatioModifier.DEFAULT; }
        |
            <NO> { modifier = MergeBlockRatioModifier.NO; }
        )
        <MERGEBLOCKRATIO>
    |
        <MERGEBLOCKRATIO> <EQ> ratio = UnsignedIntLiteral()
        [ <PERCENT> { percent = true; } ]
    )
    {
        if (ratio >= 1 && ratio <= 100) {
            return new SqlCreateAttributeMergeBlockRatio(modifier, ratio, percent, getPos());
        } else {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.numberLiteralOutOfRange(String.valueOf(ratio)));
        }
    }
}

SqlCreateAttribute CreateTableAttributeChecksum() :
{
    ChecksumEnabled checksumEnabled;
}
{
    <CHECKSUM> <EQ>
    (
        <DEFAULT_> { checksumEnabled = ChecksumEnabled.DEFAULT; }
    |
        <ON> { checksumEnabled = ChecksumEnabled.ON; }
    |
        <OFF> { checksumEnabled = ChecksumEnabled.OFF; }
    )
    { return new SqlCreateAttributeChecksum(checksumEnabled, getPos()); }
}

SqlCreateAttribute CreateTableAttributeBlockCompression() :
{
    BlockCompressionOption blockCompressionOption;
}
{
    <BLOCKCOMPRESSION> <EQ>
    (
        <DEFAULT_> { blockCompressionOption = BlockCompressionOption.DEFAULT; }
    |
        <AUTOTEMP> { blockCompressionOption = BlockCompressionOption.AUTOTEMP; }
    |
        <MANUAL> { blockCompressionOption = BlockCompressionOption.MANUAL; }
    |
        <NEVER> { blockCompressionOption = BlockCompressionOption.NEVER; }
    )
    { return new SqlCreateAttributeBlockCompression(blockCompressionOption, getPos()); }
}

SqlCreateAttribute CreateTableAttributeLog() :
{
    boolean loggingEnabled = true;
}
{
    [ <NO> { loggingEnabled = false; } ]
    <LOG> {
        return new SqlCreateAttributeLog(loggingEnabled, getPos());
    }
}

List<SqlCreateAttribute> CreateTableAttributes() :
{
    final List<SqlCreateAttribute> list = new ArrayList<SqlCreateAttribute>();
    SqlCreateAttribute e;
    Span s;
}
{
    (
        <COMMA>
        (
            e = CreateTableAttributeFallback()
        |
            e = CreateTableAttributeJournalTable()
        |
            e = CreateTableAttributeFreeSpace()
        |
            e = CreateTableAttributeIsolatedLoading()
        |
            e = CreateTableAttributeDataBlockSize()
        |
            e = CreateTableAttributeMergeBlockRatio()
        |
            e = CreateTableAttributeChecksum()
        |
            e = CreateTableAttributeBlockCompression()
        |
            e = CreateTableAttributeLog()
        |
            e = CreateTableAttributeJournal()
        ) { list.add(e); }
    )+
    { return list; }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SetType setType;
    final Volatility volatility;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final List<SqlCreateAttribute> tableAttributes;
    final SqlNodeList columnList;
    final SqlNode query;
    boolean withData = false;
    final OnCommitType onCommitType;
}
{
    setType = SetTypeOpt() volatility = VolatilityOpt() <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
        tableAttributes = CreateTableAttributes()
    |
        { tableAttributes = null; }
    )
    (
        columnList = ExtendColumnList()
    |
        { columnList = null; }
    )
    (
        <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        [
            <WITH> <DATA> {
                withData = true;
            }
        ]
    |
        { query = null; }
    )
    onCommitType = OnCommitTypeOpt()
    {
        return new SqlCreateTable(s.end(this), replace, setType, volatility, ifNotExists, id,
            tableAttributes, columnList, query, withData, onCommitType);
    }
}

/**
    Reason for having this is to be able to return the SqlExecMacro class since
    Parser.jj does not have it as a reference.

    This can also likely be extended to accomodate optional parameters later if
    need be.
*/
SqlNode SqlExecMacro() :
{
    SqlIdentifier macro;
    Span s;
}
{
    macro = CompoundIdentifier() { s = span(); }
    {
        return new SqlExecMacro(s.end(this), macro);
    }
}

SqlNode SqlUsingRequestModifier(Span s) :
{
    final SqlNodeList columnList;
}
{
    columnList = ExtendColumnList()
    {
        return new SqlUsingRequestModifier(s.end(this), columnList);
    }
}

SqlNode SqlSetTimeZoneValue() :
{
    SqlIdentifier timeZoneValue;
    SqlIdentifier name;
    Span s;
}
{
    <SET> { s = span(); }
    <TIME> <ZONE> timeZoneValue = SimpleIdentifier()
    {
        return new SqlSetTimeZone(s.end(timeZoneValue), timeZoneValue);
    }
}

SqlNode SqlInsertWithOptionalValuesKeyword() :
{
    SqlNodeList rowConstructorList;
    final Span s;
}
{
    { s = span(); }
    rowConstructorList = LiteralRowConstructorList(s)
    {
        return SqlStdOperatorTable.VALUES.createCall(s.end(this),
            rowConstructorList.toArray());
    }
}

SqlNodeList LiteralRowConstructorList(Span s) :
{
    List<SqlNode> rowList = new ArrayList<SqlNode>();
    SqlNode rowConstructor;
}
{
    rowConstructor = LiteralRowConstructor()
    { rowList.add(rowConstructor); }
    (
        LOOKAHEAD(2)
        <COMMA> rowConstructor = LiteralRowConstructor()
        { rowList.add(rowConstructor); }
    )*
    {
        return new SqlNodeList(rowList, s.end(this));
    }
}

SqlNode LiteralRowConstructor() :
{
    final Span s = Span.of();
    SqlNodeList valueList = new SqlNodeList(getPos());
    SqlNode e;
}
{
    <LPAREN>
    e = AtomicRowExpression() { valueList.add(e); }
    (
        LOOKAHEAD(2)
        <COMMA> e = AtomicRowExpression() { valueList.add(e); }
    )*
    <RPAREN>
    {
        return SqlStdOperatorTable.ROW.createCall(s.end(valueList),
            valueList.toArray());
    }
}

/* Extra operators */

<DEFAULT, DQID, BTID> TOKEN :
{
    < DATE_PART: "DATE_PART" >
|   < DATEADD: "DATEADD" >
|   < DATEDIFF: "DATEDIFF" >
|   < NEGATE: "!" >
|   < TILDE: "~" >
}

/** Parses the infix "::" cast operator used in PostgreSQL. */
void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
{
    final SqlDataTypeSpec dt;
}
{
    <INFIX_CAST> {
        checkNonQueryExpression(exprContext);
    }
    dt = DataType() {
        list.add(
            new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
                s.pos()));
        list.add(dt);
    }
}

// Parses inline MOD expression of form "x MOD y" where x, y must be numeric
SqlNode InlineModOperator() :
{
    final List<SqlNode> args = new ArrayList<SqlNode>();
    final SqlIdentifier qualifiedName;
    final Span s;
    SqlNode e;
    SqlFunctionCategory funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION;
    SqlLiteral quantifier = null;
}
{
    (
        e = NumericLiteral()
    |
        e = SimpleIdentifier()
    )
    {
        s = span();
        args.add(e);
    }
    <MOD> {
        qualifiedName = new SqlIdentifier(unquotedIdentifier(), s.pos());
    }
    (
        e = NumericLiteral()
    |
        e = SimpleIdentifier()
    )
    {
        args.add(e);
        return createCall(qualifiedName, s.end(this), funcType, quantifier, args);
    }
}

SqlNode DateTimeTerm() :
{
    final SqlNode e;
    SqlIdentifier timeZoneValue;
}
{
    (
        e = DateTimeLiteral()
    |
        e = SimpleIdentifier()
    )
    (
        <AT>
        (
            <LOCAL>
            {
                return new SqlDateTimeAtLocal(getPos(), e);
            }
        |
            <TIME> <ZONE>
            {
                timeZoneValue = SimpleIdentifier();
                return new SqlDateTimeAtTimeZone(getPos(), e, timeZoneValue);
            }
        )
    )
}
