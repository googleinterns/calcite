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
    [
        args = FunctionParameterList(ExprContext.ACCEPT_SUB_QUERY) {
            quantifier = (SqlLiteral) args.get(0);
            args.remove(0);
            return createCall(qualifiedName, s.end(this), funcType, quantifier, args);
        }
    ]
    {
        return SqlStdOperatorTable.DATE.createCall(getPos());
    }
}

SqlNode TimeFunctionCall() :
{
    final SqlIdentifier qualifiedName;
    final Span s;
}
{
    <TIME>
    {
        return SqlStdOperatorTable.TIME.createCall(getPos());
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
}

Volatility VolatilityOpt() :
{
}
{
    <VOLATILE> { return Volatility.VOLATILE; }
|
    <TEMP> { return Volatility.TEMP; }
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
            <DELETE> { onCommitType = OnCommitType.DELETE; }
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

void SourceTableAndAlias(SqlNodeList sourceTables, SqlNodeList sourceAliases) :
{
    SqlNode sourceTable;
    SqlIdentifier sourceAlias;
}
{
    sourceTable = TableRef() {
        sourceTables.add(sourceTable);
    }
    (
        [ <AS> ]
        sourceAlias = SimpleIdentifier() {
            sourceAliases.add(sourceAlias);
        }
    |
        { sourceAliases.add(null); }
    )
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

// The DateTime functions are singled out to allow for arguments to
// be parsed, such as CURRENT_DATE(0).
SqlColumnAttribute ColumnAttributeDefault() :
{
    SqlNode defaultValue;
}
{
    <DEFAULT_>
    (
        defaultValue = OptionValue()
    |
        <NULL> {
            defaultValue = SqlLiteral.createNull(getPos());
        }
    |
        defaultValue = CurrentDateFunction()
    |
        defaultValue = CurrentTimeFunction()
    |
        defaultValue = CurrentTimestampFunction()
    |
        defaultValue = ContextVariable()
    )
    {
        return new SqlColumnAttributeDefault(getPos(), defaultValue);
    }
}

/**
 * Parses a column attribute specified by the GENERATED statement.
 */
SqlColumnAttribute ColumnAttributeGenerated() :
{
    final GeneratedType generatedType;
    final List<SqlColumnAttributeGeneratedOption> generatedOptions =
        new ArrayList<SqlColumnAttributeGeneratedOption>();
}
{
    <GENERATED>
    (
        <ALWAYS> { generatedType = GeneratedType.ALWAYS; }
    |
        <BY> <DEFAULT_> { generatedType = GeneratedType.BY_DEFAULT; }
    )
    <AS> <IDENTITY>
    [
        <LPAREN>
        (
            ColumnAttributeGeneratedOption(generatedOptions)
        )+
        <RPAREN>
    ]
    {
        return new SqlColumnAttributeGenerated(getPos(), generatedType,
            generatedOptions);
    }
}

/**
 * Parses an option specified for the GENERATED column attribute, and adds it
 * to a list.
 */
void ColumnAttributeGeneratedOption(
    List<SqlColumnAttributeGeneratedOption> generatedOptions) :
{
    final SqlColumnAttributeGeneratedOption generatedOption;
}
{
    (
        generatedOption = ColumnAttributeGeneratedCycle()
    |
        generatedOption = ColumnAttributeGeneratedIncrementBy()
    |
        generatedOption = ColumnAttributeGeneratedStartWith()
    |
        generatedOption = ColumnAttributeGeneratedMaxValue()
    |
        generatedOption = ColumnAttributeGeneratedMinValue()
    )
    { generatedOptions.add(generatedOption); }
}

/**
 * Parses the CYCLE option of the GENERATED statement.
 */
SqlColumnAttributeGeneratedOption ColumnAttributeGeneratedCycle() :
{
    boolean none = false;
}
{
    [ <NO> { none = true; } ]
    <CYCLE>
    { return new SqlColumnAttributeGeneratedCycle(none); }
}

/**
 * Parses the INCREMENT BY option of the GENERATED statement.
 */
SqlColumnAttributeGeneratedOption ColumnAttributeGeneratedIncrementBy() :
{
    final SqlLiteral inc;
}
{
    <INCREMENT> <BY>
    inc = NumericLiteral()
    { return new SqlColumnAttributeGeneratedIncrementBy(inc); }
}

/**
 * Parses the START WITH option of the GENERATED statement.
 */
SqlColumnAttributeGeneratedOption ColumnAttributeGeneratedStartWith() :
{
    final SqlLiteral start;
}
{
    <START> <WITH>
    start = NumericLiteral()
    { return new SqlColumnAttributeGeneratedStartWith(start); }
}

/**
 * Parses the MAXVALUE option of the GENERATED statement.
 */
SqlColumnAttributeGeneratedOption ColumnAttributeGeneratedMaxValue() :
{
    final SqlLiteral max;
    boolean none = false;
}
{
    (
        <NO> <MAXVALUE>
        {
            max = null;
            none = true;
        }
    |
        <MAXVALUE>
        max = NumericLiteral()
    )
    { return new SqlColumnAttributeGeneratedMaxValue(max, none); }
}

/**
 * Parses the MINVALUE option of the GENERATED statement.
 */
SqlColumnAttributeGeneratedOption ColumnAttributeGeneratedMinValue() :
{
    final SqlLiteral min;
    boolean none = false;
}
{
    (
        <NO> <MINVALUE>
        {
            min = null;
            none = true;
        }
    |
        <MINVALUE>
        min = NumericLiteral()
    )
    { return new SqlColumnAttributeGeneratedMinValue(min, none); }
}

SqlColumnAttribute ColumnAttributeCharacterSet() :
{
    CharacterSet characterSet = null;
}
{
    <CHARACTER> <SET>
    (
        <LATIN> { characterSet = CharacterSet.LATIN; }
    |
        <UNICODE> { characterSet = CharacterSet.UNICODE; }
    |
        <GRAPHIC> { characterSet = CharacterSet.GRAPHIC; }
    |
        <KANJISJIS> { characterSet = CharacterSet.KANJISJIS; }
    |
        <KANJI> { characterSet = CharacterSet.KANJI; }
    )
    {
        return new SqlColumnAttributeCharacterSet(getPos(), characterSet);
    }
}

SqlColumnAttribute ColumnAttributeCaseSpecific() :
{
    boolean isCaseSpecific = true;
}
{
    [ <NOT> { isCaseSpecific = false; } ]
    <CASESPECIFIC> {
        return new SqlColumnAttributeCaseSpecific(getPos(), isCaseSpecific);
    }
}

SqlColumnAttribute ColumnAttributeUpperCase() :
{
    boolean isUpperCase = true;
}
{
    [ <NOT> { isUpperCase = false; } ]
    <UPPERCASE> {
        return new SqlColumnAttributeUpperCase(getPos(), isUpperCase);
    }
}

SqlColumnAttribute ColumnAttributeCompress() :
{
    SqlNodeList values = null;
}
{
    <COMPRESS>
    [ values = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_NONCURSOR) ]
    {
        return new SqlColumnAttributeCompress(getPos(), values);
    }
}

void ColumnAttributes(List<SqlColumnAttribute> list) :
{
    SqlColumnAttribute e;
    Span s;
}
{
    (
        (
            e = ColumnAttributeUpperCase()
        |
            e = ColumnAttributeCaseSpecific()
        |
            e = ColumnAttributeCharacterSet()
        |
            e = ColumnAttributeCompress()
        |
            e = ColumnAttributeDefault()
        |
            e = ColumnAttributeDateFormat()
        |
            e = ColumnAttributeGenerated()
        ) { list.add(e); }
    )+
}

void ColumnWithType(List<SqlNode> list) :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    List<SqlColumnAttribute> columnAttributes =
        new ArrayList<SqlColumnAttribute>();
    final Span s = Span.of();
}
{
    id = CompoundIdentifier()
    type = DataType()
    /* This structure is to support [NOT] NULL appearing anywhere in the
       declaration. Hence the list also needs to be passed in as a paramater
       rather than be the return value. If not, then the list would be overriden
       in the cases where [NOT] NULL appears inbetween other attributes. */
    (
        ColumnAttributes(columnAttributes)
    |
        nullable = IsNullable() { type = type.withNullable(nullable); }
    )*
    {
        list.add(SqlDdlNodes.column(s.add(id).end(this), id,
            type.withColumnAttributes(columnAttributes), null, null));
    }
}

SqlTableAttribute TableAttributeFallback() :
{
    boolean no = false;
    boolean protection = false;
}
{
    [ <NO>  { no = true; } ]
    <FALLBACK>
    [ <PROTECTION> { protection = true; } ]
    { return new SqlTableAttributeFallback(no, protection, getPos()); }
}

SqlTableAttribute TableAttributeJournalTable() :
{
    final SqlIdentifier id;
}
{
    <WITH> <JOURNAL> <TABLE> <EQ> id = CompoundIdentifier()
    { return new SqlTableAttributeJournalTable(id, getPos()); }
}

SqlTableAttribute TableAttributeMap() :
{
    final SqlIdentifier id;
}
{
    <MAP> <EQ> id = CompoundIdentifier()
    { return new SqlTableAttributeMap(id, getPos()); }
}

// FREESPACE attribute can take in decimals but should be truncated to an integer.
SqlTableAttribute TableAttributeFreeSpace() :
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
    { return new SqlTableAttributeFreeSpace(freeSpaceValue, percent, getPos()); }
}

/**
 * Parses FREESPACE attribute in ALTER TABLE queries.
 * Can either specify a value, or DEFAULT FREESPACE.
 */
SqlTableAttribute AlterTableAttributeFreeSpace() :
{
    SqlLiteral tempNumeric;
    int freeSpaceValue;
    boolean percent = false;
    boolean isDefault = false;
}
{
    (
        <FREESPACE> <EQ> tempNumeric = UnsignedNumericLiteral() {
            freeSpaceValue = tempNumeric.getValueAs(Integer.class);
            if (freeSpaceValue < 0 || freeSpaceValue > 75) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.numberLiteralOutOfRange(
                        String.valueOf(freeSpaceValue)));
            }
        }
        [ <PERCENT> { percent = true; } ]

    |
        <DEFAULT_> <FREESPACE>
        {
            freeSpaceValue = 0;
            isDefault = true;
        }
    )
    {
        return new SqlAlterTableAttributeFreeSpace(freeSpaceValue, percent,
            getPos(), isDefault);
    }
}

SqlTableAttribute TableAttributeIsolatedLoading() :
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
    { return new SqlTableAttributeIsolatedLoading(nonLoadIsolated, concurrent, operationLevel, getPos()); }
}

SqlTableAttribute TableAttributeJournal() :
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
    { return new SqlTableAttributeJournal(journalType, journalModifier, getPos()); }
}

SqlTableAttribute TableAttributeDataBlockSize() :
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
    {
        return new SqlTableAttributeDataBlockSize(modifier, unitSize,
            dataBlockSize, getPos());
    }
}

/**
 * Parses DATABLOCKSIZE attribute in ALTER TABLE queries,
 * including IMMEDIATE option.
 */
SqlTableAttribute AlterTableAttributeDataBlockSize() :
{
    DataBlockModifier modifier = null;
    DataBlockUnitSize unitSize;
    SqlLiteral dataBlockSize = null;
    boolean immediate = false;
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
    [ <IMMEDIATE> { immediate = true; } ]
    {
        return new SqlAlterTableAttributeDataBlockSize(modifier, unitSize,
            dataBlockSize, getPos(), immediate);
    }
}

SqlTableAttribute TableAttributeMergeBlockRatio() :
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
            return new SqlTableAttributeMergeBlockRatio(modifier, ratio, percent, getPos());
        } else {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.numberLiteralOutOfRange(String.valueOf(ratio)));
        }
    }
}

SqlTableAttribute TableAttributeChecksum() :
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
    { return new SqlTableAttributeChecksum(checksumEnabled, getPos()); }
}

/**
 * Parses CHECKSUM attribute in ALTER TABLE queries,
 * including IMMEDIATE option.
 */
SqlTableAttribute AlterTableAttributeChecksum() :
{
    ChecksumEnabled checksumEnabled;
    boolean immediate = false;
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
    [ <IMMEDIATE> { immediate = true; } ]
    {
        return new SqlAlterTableAttributeChecksum(checksumEnabled,
            getPos(), immediate);
    }
}

SqlTableAttribute TableAttributeBlockCompression() :
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
    { return new SqlTableAttributeBlockCompression(blockCompressionOption, getPos()); }
}

SqlTableAttribute TableAttributeLog() :
{
    boolean loggingEnabled = true;
}
{
    [ <NO> { loggingEnabled = false; } ]
    <LOG> {
        return new SqlTableAttributeLog(loggingEnabled, getPos());
    }
}

SqlColumnAttribute ColumnAttributeDateFormat() :
{
    SqlNode formatString = null;
}
{
    <FORMAT>
    formatString = StringLiteral()
    {
        return new SqlColumnAttributeDateFormat(getPos(), formatString);
    }
}

List<SqlTableAttribute> CreateTableAttributes() :
{
    final List<SqlTableAttribute> list = new ArrayList<SqlTableAttribute>();
    SqlTableAttribute e;
    Span s;
}
{
    (
        <COMMA>
        (
            e = TableAttributeMap()
        |
            e = TableAttributeFallback()
        |
            e = TableAttributeJournalTable()
        |
            e = TableAttributeFreeSpace()
        |
            e = TableAttributeIsolatedLoading()
        |
            e = TableAttributeDataBlockSize()
        |
            e = TableAttributeMergeBlockRatio()
        |
            e = TableAttributeChecksum()
        |
            e = TableAttributeBlockCompression()
        |
            e = TableAttributeLog()
        |
            e = TableAttributeJournal()
        ) { list.add(e); }
    )+
    { return list; }
}

WithDataType WithDataOpt() :
{
    WithDataType withData = WithDataType.WITH_DATA;
}
{
    <WITH> [ <NO> { withData = WithDataType.WITH_NO_DATA; } ] <DATA>
    { return withData; }
|
    { return WithDataType.UNSPECIFIED; }
}

SqlCreate SqlCreateTable(Span s, SqlCreateSpecifier createSpecifier) :
{
    SetType setType = SetType.UNSPECIFIED;
    Volatility volatility = Volatility.UNSPECIFIED;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final List<SqlTableAttribute> tableAttributes;
    final SqlNodeList columnList;
    final SqlNode query;
    WithDataType withData = WithDataType.UNSPECIFIED;
    SqlPrimaryIndex primaryIndex = null;
    SqlIndex index;
    List<SqlIndex> indices = new ArrayList<SqlIndex>();
    final OnCommitType onCommitType;
}
{
    [
        setType = SetTypeOpt()
        volatility = VolatilityOpt()
    |
        volatility = VolatilityOpt()
        setType = SetTypeOpt()
    |
        setType = SetTypeOpt()
    |
        volatility = VolatilityOpt()
    ]
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
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
        <AS>
        (
            query = CompoundIdentifier()
        |
            query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        )
        withData = WithDataOpt()
    |
        { query = null; }
    )
    [
        index = SqlCreateTableIndex(s) { indices.add(index); }
        (
           [<COMMA>] index = SqlCreateTableIndex(s) { indices.add(index); }
        )*
        {
            // Filter out any primary indices from index list.
            int i = 0;
            while (i < indices.size()) {
                if (indices.get(i) instanceof SqlPrimaryIndex) {
                    primaryIndex = (SqlPrimaryIndex) indices.remove(i);
                } else {
                    i++;
                }
            }
        }
    ]
    onCommitType = OnCommitTypeOpt()
    {
        return new SqlCreateTable(s.end(this), createSpecifier, setType,
         volatility, ifNotExists, id, tableAttributes, columnList, query,
         withData, primaryIndex, indices, onCommitType);
    }
}

SqlCreate SqlCreateFunctionSqlForm(Span s,
        SqlCreateSpecifier createSpecifier) :
{
    SqlIdentifier functionName = null;
    SqlNodeList fieldNames = new SqlNodeList(getPos());
    SqlNodeList fieldTypes = new SqlNodeList(getPos());
    DeterministicType isDeterministic = DeterministicType.UNSPECIFIED;
    ReactToNullInputType canRunOnNullInput = ReactToNullInputType.UNSPECIFIED;
    SqlIdentifier specificFunctionName = null;
    final SqlDataTypeSpec returnsDataType;
    boolean hasSqlSecurityDefiner = false;
    SqlLiteral tempNumeric;
    int typeInt;
    final SqlNode returnExpression;
}
{
    <FUNCTION>
    functionName = CompoundIdentifier()
    <LPAREN>
    [
        FieldNameTypeCommaListWithoutOptionalNull(fieldNames, fieldTypes)
    ]
    <RPAREN>
    <RETURNS>
    returnsDataType = DataType()
    <LANGUAGE> <SQL>
    (
        <NOT> <DETERMINISTIC>
        {
            isDeterministic = DeterministicType.NOTDETERMINISTIC;
        }
    |
        <DETERMINISTIC>
        {
            isDeterministic = DeterministicType.DETERMINISTIC;
        }
    |
        <RETURNS> <NULL> <ON> <NULL> <INPUT>
        {
            canRunOnNullInput = ReactToNullInputType.RETURNSNULL;
        }
    |
        <CALLED> <ON> <NULL> <INPUT>
        {
            canRunOnNullInput = ReactToNullInputType.CALLED;
        }
    |
        <SPECIFIC>
        {
            specificFunctionName = CompoundIdentifier();
        }
    )*
    [
        <SQL> <SECURITY> <DEFINER>
        {
            hasSqlSecurityDefiner = true;
        }
    ]
    <COLLATION> <INVOKER> <INLINE> <TYPE> tempNumeric = UnsignedNumericLiteral() {
        typeInt = tempNumeric.getValueAs(Integer.class);
        if (typeInt != 1) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.numberLiteralOutOfRange(String.valueOf(typeInt)));
        }
    }
    <RETURN> returnExpression = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
        return new SqlCreateFunctionSqlForm(s.end(this), createSpecifier,
            functionName, specificFunctionName, fieldNames, fieldTypes,
            returnsDataType, isDeterministic, canRunOnNullInput,
            hasSqlSecurityDefiner, typeInt, returnExpression);
    }
}

/**
* Parse a "name1 type1 , name2 type2 ..." list,
* the field type default is not nullable.
*/
void FieldNameTypeCommaListWithoutOptionalNull(
        SqlNodeList fieldNames,
        SqlNodeList fieldTypes) :
{
    SqlIdentifier fName;
    SqlDataTypeSpec fType;
}
{
    fName = SimpleIdentifier()
    fType = DataType()
    {
        fieldNames.add(fName);
        fieldTypes.add(fType);
    }
    (
        <COMMA>
        fName = SimpleIdentifier()
        fType = DataType()
        {
            fieldNames.add(fName);
            fieldTypes.add(fType);
        }
    )*
}

/**
 *   Parses an index declaration (both PRIMARY and non-primary indices).
 */
SqlIndex SqlCreateTableIndex(Span s) :
{
   SqlNodeList columns;
   SqlIdentifier name = null;
   boolean isUnique = false;
}
{
   (
       <NO> <PRIMARY> <INDEX>
       {
           return new SqlPrimaryIndex(s.end(this), /*columns=*/ null, /*name=*/ null,
                /*isUnique=*/ false, /*explicitNoPrimaryIndex=*/ true);
       }
   |
       [
           <UNIQUE> { isUnique = true; }
       ]
       <PRIMARY> <INDEX>
       [
           name = SimpleIdentifier()
       ]
       columns = ParenthesizedSimpleIdentifierList()
       {
           return new SqlPrimaryIndex(s.end(this), columns, name, isUnique,
                /*explicitNoPrimaryIndex=*/ false);
       }
   |
       [
            <UNIQUE> { isUnique = true; }
       ]
       <INDEX>
       [
            name = SimpleIdentifier()
       ]
       columns = ParenthesizedSimpleIdentifierList()
       {
           return new SqlSecondaryIndex(s.end(this), columns, name, isUnique);
       }
   )
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
    SqlNodeList params = new SqlNodeList(getPos());
    Span s;
}
{
    macro = CompoundIdentifier() { s = span(); }
    [
        SqlExecMacroArgument(params)
    ]
    {
        return new SqlExecMacro(s.end(this), macro, params);
    }
}

void SqlExecMacroArgument(SqlNodeList params) :
{
    SqlNode e;
}
{
    <LPAREN>
    (
        e = SqlSimpleIdentifierEqualLiteral()
        {
            params.add(e);
        }
        (
            <COMMA>
            e = SqlSimpleIdentifierEqualLiteral()
            {
                params.add(e);
            }
        )*
    |
        e = SqlExecMacroPositionalParamItem()
        {
            params.add(new SqlExecMacroParam(getPos(), e));
        }
        (
            <COMMA>
            e = SqlExecMacroPositionalParamItem()
            {
                params.add(new SqlExecMacroParam(getPos(), e));
            }
        )*
    )
    <RPAREN>
}

SqlNode SqlExecMacroPositionalParamItem() :
{
    SqlNode e;
}
{
    (
        e = AtomicRowExpression()
    |
        { e = SqlLiteral.createNull(getPos()); }
    )
    {
        return e;
    }
}

SqlNode SqlSimpleIdentifierEqualLiteral() :
{
    SqlIdentifier name;
    SqlNode value;
}
{
    name = SimpleIdentifier()
    <EQ>
    value = AtomicRowExpression()
    {
        return new SqlExecMacroParam(getPos(), name, value);
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
    e = LiteralRowConstructorItem()
    { valueList.add(e); }
    (
        <COMMA>
        e = LiteralRowConstructorItem()
        { valueList.add(e); }
    )*
    <RPAREN>
    {
        return SqlStdOperatorTable.ROW.createCall(s.end(valueList),
            valueList.toArray());
    }
}

SqlNode LiteralRowConstructorItem() :
{
    SqlNode e;
}
{
    (
        e = AtomicRowExpression()
    |
        { e = SqlLiteral.createNull(getPos()); }
    )
    {
        return e;
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

SqlNode CurrentTimestampFunction() :
{
    final List<SqlNode> args = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    <CURRENT_TIMESTAMP>
    [
        <LPAREN>
        [
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                args.add(e);
            }
        ]
        <RPAREN>
    ]
    {
        return SqlStdOperatorTable.CURRENT_TIMESTAMP.createCall(getPos(), args);
    }
}

SqlNode CurrentTimeFunction() :
{
    final List<SqlNode> args = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    <CURRENT_TIME>
    [
        <LPAREN>
        [
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                args.add(e);
            }
        ]
        <RPAREN>
    ]
    {
        return SqlStdOperatorTable.CURRENT_TIME.createCall(getPos(), args);
    }
}

SqlNode CurrentDateFunction() :
{
    final List<SqlNode> args = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    <CURRENT_DATE>
    [
        <LPAREN>
        [
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                args.add(e);
            }
        ]
        <RPAREN>
    ]
    {
        return SqlStdOperatorTable.CURRENT_DATE.createCall(getPos(), args);
    }
}

SqlNode DateTimeTerm() :
{
    final SqlNode dateTimePrimary;
    final SqlNode displacement;
}
{
    (
        dateTimePrimary = DateTimeLiteral()
    |
        dateTimePrimary = SimpleIdentifier()
    |
        dateTimePrimary = DateFunctionCall()
    )
    <AT>
    (
        <LOCAL>
        {
            return new SqlDateTimeAtLocal(getPos(), dateTimePrimary);
        }
    |
        [<TIME> <ZONE>]
        (
            displacement = SimpleIdentifier()
        |
            displacement = IntervalLiteral()
        |
            displacement = NumericLiteral()
        )
        {
            return new SqlDateTimeAtTimeZone(getPos(), dateTimePrimary, displacement);
        }
    )
}

/**
 * Parses the optional QUALIFY clause for SELECT.
 */
SqlNode QualifyOpt() :
{
    SqlNode e;
}
{
    <QUALIFY> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { return e; }
|
    { return null; }
}

// This excludes CompoundIdentifier() as a data type.
SqlDataTypeSpec DataTypeAlternativeCastSyntax() :
{
    SqlTypeNameSpec typeName;
    final Span s;
}
{
    typeName = TypeNameAlternativeCastSyntax() {
        s = span();
    }
    (
        typeName = CollectionsTypeName(typeName)
    )*
    {
        return new SqlDataTypeSpec(
            typeName,
            s.end(this));
    }
}

SqlRenameTable SqlRenameTable() :
{
    SqlIdentifier targetTable;
    SqlIdentifier sourceTable;
    RenameOption renameOption;
}
{
    <TABLE>
    targetTable = CompoundIdentifier()
    (
        <TO> { renameOption = RenameOption.TO; }
    |
        <AS> { renameOption = RenameOption.AS; }
    )
    sourceTable = CompoundIdentifier()
    {
        return new SqlRenameTable(getPos(), targetTable, sourceTable,
            renameOption);
    }
}

// This excludes CompoundIdentifier() as a type name that's found in the
// original TypeName() function. Custom data types can be parsed
// in parser.dataTypeParserMethods.
SqlTypeNameSpec TypeNameAlternativeCastSyntax() :
{
    final SqlTypeNameSpec typeNameSpec;
    final SqlIdentifier typeName;
    final Span s = Span.of();
}
{
    (
<#-- additional types are included here -->
<#-- put custom data types in front of Calcite core data types -->
<#list parser.dataTypeParserMethods as method>
        LOOKAHEAD(2)
        typeNameSpec = ${method}
    |
</#list>
        LOOKAHEAD(2)
        typeNameSpec = SqlTypeName(s)
    |
        typeNameSpec = RowTypeName()
    )
    {
        return typeNameSpec;
    }
}

SqlNode AlternativeTypeConversionLiteralOrIdentifier() :
{
     final SqlNode q;
     final SqlNode e;
}
{
    (
        q = Literal()
    |
        q = CompoundIdentifier()
    )
    e = AlternativeTypeConversionQuery(q) { return e; }
}

SqlNode AlternativeTypeConversionQuery(SqlNode q) :
{
    final Span s = span();
    final List<SqlNode> args = startList(q);
    final SqlDataTypeSpec dt;
    final SqlNode interval;
    final SqlNode format;
}
{
    <LPAREN>
    (
        dt = DataTypeAlternativeCastSyntax() { args.add(dt); }
    |
        <INTERVAL> interval = IntervalQualifier() { args.add(interval); }
    )
    [ <FORMAT> format = StringLiteral() { args.add(format); } ]
    <RPAREN> {
        return SqlStdOperatorTable.CAST.createCall(s.end(this), args);
    }
}

SqlNode InlineFormatLiteralOrIdentifier() :
{
     final SqlNode q;
     final SqlNode e;
}
{
    (
        q = Literal()
    |
        q = CompoundIdentifier()
    )
    e = InlineFormatQuery(q) { return e; }
}

SqlNode InlineFormatQuery(SqlNode q) :
{
    final Span s = span();
    final SqlNode format;
}
{
    <LPAREN> <FORMAT> format = StringLiteral() <RPAREN>
    {
        return SqlStdOperatorTable.FORMAT.createCall(s.end(this), q, format);
    }
}

SqlNode NamedLiteralOrIdentifier() :
{
     final SqlNode q;
     final SqlNode e;
}
{
    (
        q = Literal()
    |
        q = CompoundIdentifier()
    )
    e = NamedQuery(q) { return e; }
}

SqlNode NamedQuery(SqlNode q) :
{
    final Span s = span();
    final SqlIdentifier name;
}
{
    <LPAREN> <NAMED> name = SimpleIdentifier() <RPAREN>
    {
        return SqlStdOperatorTable.AS.createCall(s.end(this), q, name);
    }
}

/**
 * The RANK function call with sorting expressions has a default ordering of DESC
 * while the RANK() OVER function call has a default ordering of ASC.
 */
SqlNode RankSortingExpression() :
{
    SqlNode e;
}
{
    e = Expression(ExprContext.ACCEPT_SUB_QUERY)
    (
        <ASC>
    |
        [ <DESC> ]  {
            e = SqlStdOperatorTable.DESC.createCall(getPos(), e);
        }
    )
    { return e; }
}

// Parse RANK function call with sorting expressions.
SqlNode RankFunctionCallWithParams() :
{
    final SqlFunctionCategory funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION;
    final SqlNodeList orderByList = new SqlNodeList(SqlParserPos.ZERO);
    final SqlIdentifier qualifiedName;
    final SqlNode rankCall;
    final SqlNode over;
    final Span s;
    final Span s1;
    SqlNode e;
}
{
    <RANK> {
        s = span();
        qualifiedName = new SqlIdentifier(unquotedIdentifier(), getPos());
        rankCall = createCall(qualifiedName, s.end(this), funcType, null, Collections.emptyList());
    }
    <LPAREN> { s1 = span(); }
    e = RankSortingExpression() { orderByList.add(e); }
    (
        <COMMA> e = RankSortingExpression() {
            orderByList.add(e);
        }
    )*
    <RPAREN> {
        over = SqlWindow.create(null, null, SqlNodeList.EMPTY, orderByList,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO), null, null, null, s1.end(this));
        return SqlStdOperatorTable.OVER.createCall(s.end(over), rankCall, over);
    }
}

/**
 * Parses ALTER TABLE queries.
 */
SqlAlter SqlAlterTable(Span s, String scope) :
{
     final SqlIdentifier tableName;
     final List<SqlTableAttribute> tableAttributes;
     final List<SqlAlterTableOption> alterTableOptions;
}
{
    <TABLE>
    tableName = SimpleIdentifier()
    (
        tableAttributes = AlterTableAttributes()
        (
            alterTableOptions = AlterTableOptions()
        |
            { alterTableOptions = null; }
        )
    |
        alterTableOptions = AlterTableOptions()
        { tableAttributes = null; }
    )
    {
        return new SqlAlterTable(getPos(), scope, tableName,
            tableAttributes, alterTableOptions);
    }
}

/**
 * Parses table attributes for ALTER TABLE queries.
 */
List<SqlTableAttribute> AlterTableAttributes() :
{
    final List<SqlTableAttribute> list = new ArrayList<SqlTableAttribute>();
    SqlTableAttribute e;
    Span s;
}
{
    (
        <COMMA>
        (
            e = AlterTableAttributeOnCommit()
        |
            e = TableAttributeFallback()
        |
            e = TableAttributeJournalTable()
        |
            e = AlterTableAttributeFreeSpace()
        |
            e = AlterTableAttributeDataBlockSize()
        |
            e = TableAttributeMergeBlockRatio()
        |
            e = AlterTableAttributeChecksum()
        |
            e = TableAttributeBlockCompression()
        |
            e = TableAttributeLog()
        |
            e = TableAttributeJournal()
        ) { list.add(e); }
    )+
    { return list; }
}

/**
 * Parses the ON COMMIT attribute for ALTER TABLE queries.
 */
SqlTableAttribute AlterTableAttributeOnCommit() :
{
    final OnCommitType onCommitType;
}
{
    <ON> <COMMIT>
    (
        <PRESERVE> { onCommitType = OnCommitType.PRESERVE; }
    |
        <DELETE> { onCommitType = OnCommitType.DELETE; }
    )
    <ROWS>
    { return new SqlAlterTableAttributeOnCommit(getPos(), onCommitType); }
}

/**
 * Parses a list of alter options (ex. ADD, DROP, RENAME) for
 * ALTER TABLE queries.
 */
List<SqlAlterTableOption> AlterTableOptions() :
{
    final List<SqlAlterTableOption> alterTableOptions =
        new ArrayList<SqlAlterTableOption>();
    SqlAlterTableOption alterTableOption;
}
{
    alterTableOption = AlterTableOption()
    { alterTableOptions.add(alterTableOption); }
    (
        <COMMA>
        alterTableOption = AlterTableOption()
        { alterTableOptions.add(alterTableOption); }
    )*
    { return alterTableOptions; }
}

/**
 * Parses a single alter option (ex. ADD, DROP, RENAME) for
 * ALTER TABLE queries.
 * Used by {@code AlterTableOptions}.
 */
SqlAlterTableOption AlterTableOption() :
{
    final SqlAlterTableOption option;
}
{
    (
        option = AlterTableAddColumns()
    |
        option = AlterTableRename()
    |
        option = AlterTableDrop()
    )
    { return option; }
}

/**
 * Parses an ADD column statement within an ALTER TABLE query.
 * Handles both the case where there is a single column not enclosed in
 * parentheses, and the case where there are one or more columns enclosed
 * in parentheses.
 */
SqlAlterTableOption AlterTableAddColumns() :
{
    final List<SqlNode> columnList = new ArrayList<SqlNode>();
    final SqlNodeList columns;
    final Span s;
}
{
    <ADD>
    (
        { s = span(); }
        ColumnWithType(columnList)
        {
            columns = new SqlNodeList(columnList, s.end(this));
        }
    |
        columns = ExtendColumnList()
    )
    { return new SqlAlterTableAddColumns(columns); }
}

/**
 * Parses a RENAME statement within an ALTER TABLE query.
 */
SqlAlterTableOption AlterTableRename() :
{
    final SqlIdentifier origName;
    final SqlIdentifier newName;
}
{
    <RENAME>
    origName = SimpleIdentifier()
    <TO>
    newName = SimpleIdentifier()
    { return new SqlAlterTableRename(origName, newName); }
}

/**
 * Parses a DROP statement within an ALTER TABLE query.
 */
SqlAlterTableOption AlterTableDrop() :
{
    final SqlIdentifier dropObj;
    boolean identity = false;
}
{
    <DROP>
    dropObj = SimpleIdentifier()
    [
        <IDENTITY> { identity = true; }
    ]
    { return new SqlAlterTableDrop(dropObj, identity); }
}

/**
 * Parses a TOP N statement in a SELECT query
 * (for example SELECT TOP 5 * FROM FOO).
 */
SqlNode SqlSelectTopN(SqlParserPos pos) :
{
    final SqlNumericLiteral selectNum;
    final double tempNum;
    boolean isPercent = false;
    boolean withTies = false;
}
{
    <TOP>
    selectNum = UnsignedNumericLiteral()
    { tempNum = selectNum.getValueAs(Double.class); }
    [
        <PERCENT>
        {
            isPercent = true;
            if (tempNum > 100) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.numberLiteralOutOfRange(String.valueOf(tempNum)));
            }
        }
    ]
    {
        if (tempNum != Math.floor(tempNum) && !isPercent) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.integerRequiredWhenNoPercent(
                    String.valueOf(tempNum)
                ));
        }
    }
    [
        <WITH> <TIES> { withTies = true; }
    ]
    {
        return new SqlSelectTopN(pos, selectNum,
            SqlLiteral.createBoolean(isPercent, pos),
            SqlLiteral.createBoolean(withTies, pos));
    }
}

SqlHostVariable SqlHostVariable() :
{
    final String name;
}
{
    <COLON>
    (
        <IDENTIFIER> { name = unquotedIdentifier(); }
    |
        name = NonReservedKeyWord()
    )
    { return new SqlHostVariable(name, getPos()); }
}
