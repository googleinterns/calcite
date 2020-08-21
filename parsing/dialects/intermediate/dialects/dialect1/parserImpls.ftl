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

/* Extra operators */

<DEFAULT, DQID, BTID> TOKEN :
{
    < DATE_PART: "DATE_PART" >
|   < DATEADD: "DATEADD" >
|   < DATEDIFF: "DATEDIFF" >
|   < NEGATE: "!" >
|   < TILDE: "~" >
}

SqlTypeNameSpec ByteIntType() :
{
    final Span s = Span.of();
}
{
    <BYTEINT>
    {
        return new SqlBasicTypeNameSpec(SqlTypeName.BYTEINT, s.end(this));
    }
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

SqlNode DateAddFunctionCall() :
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
    ( <TEMP> | <GLOBAL> <TEMPORARY> ) { return Volatility.TEMP; }
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
        <DATE> <QUOTED_STRING> {
            defaultValue = SqlParserUtil.parseDateLiteral(token.image,
                getPos());
        }
    |
        defaultValue = DateFunctionCall()
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
        <KANJI1> { characterSet = CharacterSet.KANJI1; }
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
    SqlNode value = null;
}
{
    <COMPRESS>
    [
        value = StringLiteral()
    |
        value = NumericLiteral()
    |
        value = ParenthesizedCompressCommaList()
    ]
    {
        return new SqlColumnAttributeCompress(getPos(), value);
    }
}

SqlNodeList ParenthesizedCompressCommaList() :
{
    final List<SqlNode> values = new ArrayList<SqlNode>();
    SqlNode value = null;
}
{
    <LPAREN>
    value = CompressOption() { values.add(value); }
    (
        <COMMA>
        value = CompressOption() { values.add(value); }
    )*
    <RPAREN>
    {
        return new SqlNodeList(values, getPos());
    }
}

SqlNode CompressOption() :
{
    SqlNode value;
}
{
    (
        value = StringLiteral()
    |
        value = NumericLiteral()
    |
        value = DateTimeLiteral()
    |
        <NULL> {
            value = SqlLiteral.createNull(getPos());
        }
    )
    {
        return value;
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
        |
            e = ColumnAttributeTitle()
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
    SqlIdentifier colocateName = null;
}
{
    <MAP> <EQ> id = CompoundIdentifier()
    [ <COLOCATE> <USING> colocateName = CompoundIdentifier() ]
    { return new SqlTableAttributeMap(id, colocateName, getPos()); }
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
        ( <ON> | <ALL> | <LOW> | <MEDIUM> | <HIGH> )
        { checksumEnabled = ChecksumEnabled.ON; }
    |
        ( <OFF> | <NONE> ) { checksumEnabled = ChecksumEnabled.OFF; }
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

SqlColumnAttribute ColumnAttributeNamed() :
{
    SqlNode namedString = null;
}
{
    <NAMED>
    namedString = StringLiteral()
    {
        return new SqlColumnAttributeNamed(getPos(), namedString);
    }
}

SqlColumnAttribute ColumnAttributeTitle() :
{
    final SqlNode titleString;
}
{
    <TITLE>
    titleString = StringLiteral()
    {
        return new SqlColumnAttributeTitle(getPos(), titleString);
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

SqlCreate SqlCreateTable() :
{
    final Span s;
    SqlCreateSpecifier createSpecifier = SqlCreateSpecifier.CREATE;
    SetType tmpSetType;
    SetType setType = SetType.UNSPECIFIED;
    Volatility tmpVolatility;
    Volatility volatility = Volatility.UNSPECIFIED;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final List<SqlTableAttribute> tableAttributes;
    final SqlNodeList columnList;
    final SqlNode query;
    WithDataType withData = WithDataType.UNSPECIFIED;
    SqlPrimaryIndex primaryIndex = null;
    SqlIndex index;
    SqlTablePartition partition = null;
    List<SqlIndex> indices = new ArrayList<SqlIndex>();
    final OnCommitType onCommitType;
}
{
    (
        <CT> { s = span(); }
    |
        <CREATE> { s = span(); }
        [
            <OR> <REPLACE> {
                createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE;
            }
        ]
        (
            tmpSetType = SetTypeOpt()
            {
                if (setType != SetType.UNSPECIFIED) {
                    throw SqlUtil.newContextException(s.pos(),
                        RESOURCE.illegalSetType());
                }
                setType = tmpSetType;
            }
        |
            tmpVolatility = VolatilityOpt()
            {
                if (volatility != Volatility.UNSPECIFIED) {
                    throw SqlUtil.newContextException(s.pos(),
                        RESOURCE.illegalVolatility());
                }
                volatility = tmpVolatility;
            }
        )*
        <TABLE>
    )
    ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
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
        (
            index = SqlCreateTableIndex(s) { indices.add(index); }
        |
            <PARTITION> <BY>
            partition = CreateTablePartitionBy()
        )
        (
           [ <COMMA> ]
           (
               index = SqlCreateTableIndex(s) { indices.add(index); }
           |
               <PARTITION> <BY>
               partition = CreateTablePartitionBy()
           )
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
        return new SqlCreateTableDialect1(s.end(this), createSpecifier, setType,
         volatility, ifNotExists, id, tableAttributes, columnList, query,
         withData, primaryIndex, indices, partition, onCommitType);
    }
}

SqlTablePartition CreateTablePartitionBy() :
{
    final SqlNodeList partitions = new SqlNodeList(getPos());
    SqlNode e;
}
{
    (
        e = PartitionExpression()
        { partitions.add(e); }
    |
        <LPAREN>
        e = PartitionExpression()
        { partitions.add(e); }
        (
            <COMMA>
            e = PartitionExpression()
            { partitions.add(e); }
        )*
        <RPAREN>
    )
    { return new SqlTablePartition(getPos(), partitions); }
}

SqlNode  PartitionExpression() :
{
    final SqlNode e;
    int constant = 0;
}
{
    (
        e = RangeN()
    |
        e = CaseN()
    |
        e = SqlExtractFromDateTime()
    |
        e = PartitionByColumnOption()
    |
        e = AtomicRowExpression()
    )
    [
        <ADD> { constant = UnsignedIntLiteral(); }
    ]
    { return new SqlTablePartitionExpression(getPos(), e, constant); }
}

SqlNode PartitionByColumnOption() :
{
    SqlNode e;
    boolean containsAllButSpecifier = false;
    final SqlNodeList columnList = new SqlNodeList(getPos());
}
{
    (
        <COLUMN>
        [ <ALL> <BUT> { containsAllButSpecifier = true; } ]
        <LPAREN>
        e = PartitionColumnItem()
        { columnList.add(e); }
        (
            <COMMA>
            e = PartitionColumnItem()
            { columnList.add(e); }
        )*
        <RPAREN>
    |
        <COLUMN>
    |
        <COLUMN>
        e = SimpleIdentifier()
        { columnList.add(e); }
    )
    {
        return new SqlTablePartitionByColumn(getPos(), columnList,
            containsAllButSpecifier);
    }
}

SqlNode PartitionColumnItem() :
{
    SqlNode e;
    final SqlNodeList args = new SqlNodeList(getPos());
    CompressionOpt compressionOpt = CompressionOpt.NOT_SPECIFIED;
}
{
    (
        <ROW> <LPAREN>
        e = SimpleIdentifier()
        { args.add(e); }
        (
            <COMMA>
            e = SimpleIdentifier()
            { args.add(e); }
        )*
        <RPAREN>
    |
         <ROW>
         e = SimpleIdentifier()
         { args.add(e); }
    )
    [
        <AUTO> <COMPRESS> { compressionOpt = CompressionOpt.AUTO_COMPRESS; }
    |
        <NO> <AUTO> <COMPRESS>
        { compressionOpt = CompressionOpt.NO_AUTO_COMPRESS; }
    ]
    {
        return new SqlTablePartitionRowFormat(getPos(),args, compressionOpt);
    }
|
    e = SimpleIdentifier()
    { return e; }
}

SqlNode SqlExtractFromDateTime() :
{
    List<SqlNode> args = null;
    SqlNode e;
    final Span s;
    final TimeUnit unit;
}
{
    <EXTRACT> {
        s = span();
    }
    <LPAREN>
    (
        <NANOSECOND> { unit = TimeUnit.NANOSECOND; }
    |
        <MICROSECOND> { unit = TimeUnit.MICROSECOND; }
    |
        unit = TimeUnit()
    )
    { args = startList(new SqlIntervalQualifier(unit, null, getPos())); }
    <FROM>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
    <RPAREN> {
        return SqlStdOperatorTable.EXTRACT.createCall(s.end(this), args);
    }
}

SqlCreate SqlCreateMacro() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final SqlIdentifier macroName;
    final SqlNodeList attributes;
    final SqlNodeList sqlStatements;
}
{
    (
        <CREATE> { createSpecifier = SqlCreateSpecifier.CREATE; }
    |
        <REPLACE> { createSpecifier = SqlCreateSpecifier.REPLACE; }
    )
    {
        s = span();
    }
    <MACRO> macroName = CompoundIdentifier()
    (
        attributes = ExtendColumnList()
    |
        { attributes = null; }
    )
    <AS>
    <LPAREN>
    sqlStatements = SqlStmtList()
    <RPAREN>
    {
        return new SqlCreateMacro(s.end(this), createSpecifier, macroName,
            attributes, sqlStatements);
    }
}

SqlCreate SqlCreateFunctionSqlForm() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
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
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
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

SqlNode InlineModOperatorLiteralOrIdentifier() :
{
    final SqlNode e;
    final SqlNode q;
}
{
    (
        e = NumericLiteral()
    |
        e = CompoundIdentifier()
    )
    q = InlineModOperator(e) { return q; }
}

// Parses inline MOD expression of form "x MOD y" where x, y must be numeric
SqlNode InlineModOperator(SqlNode q) :
{
    final List<SqlNode> args = new ArrayList<SqlNode>();
    final Span s;
    SqlNode e;
}
{
    <MOD> {
        s = span();
        args.add(q);
    }
    (
        e = NumericLiteral()
    |
        e = CompoundIdentifier()
    |
        e = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_SUB_QUERY)
        { e = ((SqlNodeList) e).get(0); }
    )
    {
        args.add(e);
        return SqlStdOperatorTable.MOD.createCall(s.end(this), args);
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
            displacement = StringLiteral()
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
    final SqlIdentifier oldTable;
    final SqlIdentifier newTable;
}
{
    <TABLE>
    oldTable = CompoundIdentifier()
    (
        <TO>
    |
        <AS>
    )
    newTable = CompoundIdentifier()
    {
        return new SqlRenameTable(getPos(), oldTable, newTable);
    }
}

SqlRenameMacro SqlRenameMacro() :
{
    final SqlIdentifier oldMacro;
    final SqlIdentifier newMacro;
}
{
    <MACRO>
    oldMacro = CompoundIdentifier()
    (
        <TO>
    |
        <AS>
    )
    newMacro = CompoundIdentifier()
    {
        return new SqlRenameMacro(getPos(), oldMacro, newMacro);
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
        LOOKAHEAD(2)
        typeNameSpec = BlobDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = ByteDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = ByteIntType()
    |
        LOOKAHEAD(2)
        typeNameSpec = ClobDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = NumberDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = SqlJsonDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = SqlPeriodDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = VarbyteDataType()
    |
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

SqlColumnAttribute AlternativeTypeConversionAttribute():
{
    SqlColumnAttribute e;
}
{
    (
        e = ColumnAttributeUpperCase()
    |
        e = ColumnAttributeCharacterSet()
    |
        e = ColumnAttributeDateFormat()
    |
        e = ColumnAttributeTitle()
    |
        e = ColumnAttributeNamed()
    )
    { return e; }
}

void AlternativeTypeConversionAttributeList(List<SqlNode> attributes):
{
    SqlColumnAttribute e;
}
{
    e = AlternativeTypeConversionAttribute() { attributes.add(e); }
    (
        <COMMA>
        e = AlternativeTypeConversionAttribute() { attributes.add(e); }
    )*
}

SqlNode AlternativeTypeConversionQuery(SqlNode q) :
{
    final Span s = span();
    List<SqlNode> args;
    SqlDataTypeSpec dt;
    SqlNode interval;
}
{
    (
        <LPAREN> { args = startList(q); }
        (
            (
                dt = DataTypeAlternativeCastSyntax() { args.add(dt); }
            |
                <INTERVAL> interval = IntervalQualifier()
                {
                    args.add(interval);
                }
            )
            [ <COMMA> AlternativeTypeConversionAttributeList(args) ]
        |
            AlternativeTypeConversionAttributeList(args)
            [
                (
                    <COMMA> dt = DataTypeAlternativeCastSyntax()
                    {
                        args.add(1, dt);
                    }
                |
                    <COMMA> <INTERVAL> interval = IntervalQualifier()
                    {
                        args.add(1, interval);
                    }
                )
                [ <COMMA> AlternativeTypeConversionAttributeList(args) ]
            ]
        )
        <RPAREN> {
            q = SqlStdOperatorTable.CAST.createCall(s.end(this), args);
        }
    )+
    { return q; }
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
    tableName = CompoundIdentifier()
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

SqlNode InlineCaseSpecific() :
{
    final SqlNode value;
    final SqlCall caseSpecific;
}
{
    (
        value = StringLiteral()
    |
        LOOKAHEAD( CompoundIdentifier() CaseSpecific() )
        value = CompoundIdentifier()
    )
    caseSpecific = CaseSpecific(value)
    {
        return caseSpecific;
    }
}

SqlCall CaseSpecific(SqlNode value) :
{
    boolean includeNot = false;
}
{
    <LPAREN>
    [ <NOT> { includeNot = true; } ]
    (
        <CASESPECIFIC>
    |
        <CS>
    )
    <RPAREN>
    {
        return includeNot
            ? SqlStdOperatorTable.NOT_CASE_SPECIFIC.createCall(getPos(), value)
            : SqlStdOperatorTable.CASE_SPECIFIC.createCall(getPos(), value);
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
        name = NonReservedKeyword()
    )
    { return new SqlHostVariable(name, getPos()); }
}

SqlTypeNameSpec SqlJsonDataType() :
{
    SqlLiteral tempLiteral;
    Integer maxLength = null;
    Integer inlineLength = null;
    CharacterSet characterSet = null;
    StorageFormat storageFormat = null;
}
{
    <JSON>
    [
        <LPAREN>
        tempLiteral = UnsignedNumericLiteral() {
            maxLength = tempLiteral.getValueAs(Integer.class);
            if (maxLength < 2) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.numberLiteralOutOfRange(String.valueOf(maxLength)));
            }
        }
        <RPAREN>
    ]
    (
        (
            <INLINE> <LENGTH>
            tempLiteral = UnsignedNumericLiteral() {
                inlineLength = tempLiteral.getValueAs(Integer.class);
                if ((maxLength != null && maxLength < inlineLength) ||
                    inlineLength <= 0) {
                    throw SqlUtil.newContextException(getPos(),
                        RESOURCE.numberLiteralOutOfRange(
                            String.valueOf(inlineLength)));
                }
            }
        )
    |
        (
            <CHARACTER> <SET>
            (
                <LATIN> {
                    characterSet = CharacterSet.LATIN;
                }
            |
                <UNICODE> {
                    characterSet = CharacterSet.UNICODE;
                }
            )
        )
    |
        (
            <STORAGE> <FORMAT>
            (
                <BSON> {
                    storageFormat = StorageFormat.BSON;
                }
            |
                <UBJSON> {
                    storageFormat = StorageFormat.UBJSON;
                }
            )
        )
    )*
    {
        if (characterSet != null && storageFormat != null) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.illegalQueryExpression());
        }
        return new SqlJsonTypeNameSpec(getPos(), maxLength, inlineLength,
            characterSet, storageFormat);
    }
}

SqlNode SqlHexCharStringLiteral() :
{
    final String hex;
    final String formatString;
    String charSet = null;
}
{
    (
        <PREFIXED_HEX_STRING_LITERAL>
        {
            charSet = SqlParserUtil.getCharacterSet(token.image);
        }
    |
        <QUOTED_HEX_STRING>
    )
    {
        // In the case of matching "PREFIXED_HEX_STRING_LITERAL" or
        // "QUOTED_HEX_STRING" token, it is guaranteed that the following
        // Java string manipulation logic is valid.
        String[] tokens = token.image.split("'");
        hex = tokens[1];
        formatString = tokens[2];
        return new SqlHexCharStringLiteral(hex, getPos(), charSet,
            formatString);
    }
}

SqlTypeNameSpec ByteDataType() :
{
    final Span s = Span.of();
    int precision = -1;
}
{
    <BYTE>
    [ precision = VariableBinaryTypePrecision() ]
    {
        return new SqlBasicTypeNameSpec(SqlTypeName.BYTE, precision, s.end(this));
    }
}

SqlTypeNameSpec VarbyteDataType() :
{
    final Span s = Span.of();
    final int precision;
}
{
    <VARBYTE>
    precision = VariableBinaryTypePrecision()
    {
        return new SqlBasicTypeNameSpec(SqlTypeName.VARBYTE, precision, s.end(this));
    }
}

int VariableBinaryTypePrecision() :
{
    final int precision;
}
{
    <LPAREN>
    precision = UnsignedIntLiteral()
    {
        if (precision > 64000) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.numberLiteralOutOfRange(String.valueOf(precision)));
        }
    }
    <RPAREN>
    { return precision; }
}

/**
 * Parses LIKE ANY, LIKE ALL, and LIKE SOME statements, and appends operands
 * to given list.
 */
void LikeAnyAllSome(List<Object> list, Span s) :
{
    final SqlOperator op;
    final SqlKind kind;
    boolean notLike = false;
    SqlNodeList nodeList;
}
{
    [ <NOT> { notLike = true; } ]
    <LIKE>
    (
        ( <SOME> | <ANY> ) { kind = SqlKind.SOME; }
    |
        <ALL> { kind = SqlKind.ALL; }
    )
    {
        op = SqlStdOperatorTable.like(kind, notLike);
        s.clear().add(this);
    }
    nodeList = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_NONCURSOR)
    {
        list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
        s.add(nodeList);
        if (nodeList.size() == 1) {
            SqlNode item = nodeList.get(0);
            if (item.isA(SqlKind.QUERY)) {
                list.add(item);
            } else {
                list.add(nodeList);
            }
        } else {
            list.add(nodeList);
        }
    }
}

SqlTypeNameSpec SqlPeriodDataType() :
{
    final TimeScale timeScale;
    SqlNumericLiteral precision = null;
    boolean isWithTimezone = false;
}
{
    <PERIOD> <LPAREN>
    (
        <DATE> { timeScale = TimeScale.DATE; }
    |
        (
            <TIME> { timeScale = TimeScale.TIME; }
        |
            <TIMESTAMP> { timeScale = TimeScale.TIMESTAMP; }
        )
        [
            <LPAREN>
            precision = UnsignedNumericLiteral()
            <RPAREN>
        ]
        [
            <WITH> <TIME> <ZONE> { isWithTimezone = true; }
        ]
    )
    <RPAREN>
    {
        return new SqlPeriodTypeNameSpec(timeScale, precision, isWithTimezone,
            getPos());
    }
}

SqlBlobTypeNameSpec BlobDataType() :
{
    SqlLiteral maxLength = null;
    SqlLobUnitSize unitSize = SqlLobUnitSize.UNSPECIFIED;
}
{
    (
        <BLOB>
    |
        <BINARY> <LARGE> <OBJECT>
    )
    [
        <LPAREN>
        <UNSIGNED_INTEGER_LITERAL>
        {
            maxLength = SqlLiteral.createExactNumeric(token.image, getPos());
        }
        [ unitSize = LobUnitSize() ]
        <RPAREN>
    ]
    { return new SqlBlobTypeNameSpec(maxLength, unitSize, getPos()); }
}

SqlClobTypeNameSpec ClobDataType() :
{
    SqlLiteral maxLength = null;
    SqlLobUnitSize unitSize = SqlLobUnitSize.UNSPECIFIED;
    CharacterSet characterSet = null;
}
{
    (
        <CLOB>
    |
        <CHARACTER> <LARGE> <OBJECT>
    )
    [
        <LPAREN>
        <UNSIGNED_INTEGER_LITERAL>
        {
            maxLength = SqlLiteral.createExactNumeric(token.image, getPos());
        }
        [ unitSize = LobUnitSize() ]
        <RPAREN>
    ]
    [
        ( <CHARACTER> | <CHAR> ) <SET>
        (
            <LATIN> { characterSet = CharacterSet.LATIN; }
        |
            <UNICODE> { characterSet = CharacterSet.UNICODE; }
        )
    ]
    { return new SqlClobTypeNameSpec(maxLength, unitSize, characterSet, getPos()); }
}

SqlLobUnitSize LobUnitSize() :
{
    final SqlLobUnitSize unitSize;
}
{
    (
        <K> { unitSize = SqlLobUnitSize.K; }
    |
        <M> { unitSize = SqlLobUnitSize.M; }
    |
        <G> { unitSize = SqlLobUnitSize.G; }
    )
    { return unitSize; }
}

SqlNumberTypeNameSpec NumberDataType() :
{
    boolean isPrecisionStar = false;
    SqlLiteral precision = null;
    SqlLiteral scale = null;
}
{
    <NUMBER>
    [
        <LPAREN>
        (
            <UNSIGNED_INTEGER_LITERAL>
            {
                precision = SqlLiteral.createExactNumeric(token.image, getPos());
            }
        |
            <STAR> { isPrecisionStar = true; }
        )
        [
            <COMMA> <UNSIGNED_INTEGER_LITERAL>
            {
                scale = SqlLiteral.createExactNumeric(token.image, getPos());
            }
        ]
        <RPAREN>
    ]
    { return new SqlNumberTypeNameSpec(isPrecisionStar, precision, scale, getPos()); }
}

SqlDrop SqlDropMacro(Span s, boolean replace) :
{
    final SqlIdentifier id;
}
{
    <MACRO> id = CompoundIdentifier() {
        return SqlDdlNodes.dropMacro(s.end(this), false, id);
    }
}

List<SqlTableAttribute> CreateJoinIndexTableAttributes() :
{
    final List<SqlTableAttribute> list = new ArrayList<SqlTableAttribute>();
    SqlTableAttribute e;
}
{
    (
        <COMMA>
        (
            e = TableAttributeMap()
        |
            e = TableAttributeFallback()
        |
            e = TableAttributeChecksum()
        |
            e = TableAttributeBlockCompression()
        ) { list.add(e); }
    )*
    { return list; }
}

SqlCreateJoinIndex SqlCreateJoinIndex() :
{
    final Span s;
    final SqlIdentifier name;
    final List<SqlTableAttribute> tableAttributes;
    final SqlNode select;
    final List<SqlIndex> indices = new ArrayList<SqlIndex>();
    SqlIndex index;
}
{
    <CREATE> { s = span(); }
    <JOIN> <INDEX> name = CompoundIdentifier()
    tableAttributes = CreateJoinIndexTableAttributes()
    <AS>
    select = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    [
        index = SqlCreateTableIndex(s) { indices.add(index); }
        (
           [ <COMMA> ] index = SqlCreateTableIndex(s) { indices.add(index); }
        )*
    ]
    {
        return new SqlCreateJoinIndex(s.end(this), name, tableAttributes,
            select, indices);
    }
}

/** As ParenthesizedQueryOrCommaList, but allows DEFAULT
 * in place of any of the expressions. For example,
 * {@code (x, DEFAULT, null, DEFAULT)}. */
SqlNodeList ParenthesizedQueryOrCommaListWithDefault(
    ExprContext exprContext) :
{
    SqlNode e;
    List<SqlNode> list;
    ExprContext firstExprContext = exprContext;
    final Span s;
}
{
    <LPAREN>
    {
        // we've now seen left paren, so a query by itself should
        // be interpreted as a sub-query
        s = span();
        switch (exprContext) {
        case ACCEPT_SUB_QUERY:
            firstExprContext = ExprContext.ACCEPT_NONCURSOR;
            break;
        case ACCEPT_CURSOR:
            firstExprContext = ExprContext.ACCEPT_ALL;
            break;
        }
    }
    (
        e = OrderedQueryOrExpr(firstExprContext)
    |
        e = Default()
    |
        // This LOOKAHEAD ensures that parsing fails if there is an empty set of
        // parentheses after a VALUES keyword since that is invalid syntax in
        // most dialects
        LOOKAHEAD({ getToken(1).kind != RPAREN })
        { e = SqlLiteral.createNull(getPos()); }
    )
    {
        list = startList(e);
    }
    (
        <COMMA>
        {
            // a comma-list can't appear where only a query is expected
            checkNonQueryExpression(exprContext);
        }
        (
            e = Expression(exprContext)
        |
            e = Default()
        |
            { e = SqlLiteral.createNull(getPos()); }
        )
        {
            list.add(e);
        }
    )*
    <RPAREN>
    {
        return new SqlNodeList(list, s.end(this));
    }
}

/**
 * Parses an SQL statement. SqlUpsert() must be parsed before SqlUpdate() since
 * it uses a LOOKAHEAD for SqlUpdate(). OrderedQueryOrExpr() must also be parsed
 * at the end or it will attempt to parse some statements such as "UPD
 * expressions.
 */
SqlNode SqlStmt() :
{
    SqlNode stmt;
}
{
    (
        stmt = SqlAlter()
    |
        stmt = SqlCreate()
    |
        stmt = SqlDelete()
    |
        stmt = SqlDescribe()
    |
        stmt = SqlDrop()
    |
        stmt = SqlExec()
    |
        stmt = SqlExplain()
    |
        stmt = SqlHelp()
    |
        stmt = SqlInsert()
    |
        stmt = SqlMerge()
    |
        stmt = SqlRename()
    |
        stmt = SqlProcedureCall()
    |
        stmt = SqlSetOption(Span.of(), null)
    |
        stmt = SqlSetTimeZone()
    |
        LOOKAHEAD(SqlUpdate() <ELSE>)
        stmt = SqlUpsert()
    |
        stmt = SqlUpdate()
    |
        stmt = SqlUsing()
    |
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return stmt;
    }
}

/**
 * Parses a leaf SELECT expression without ORDER BY.
 */
SqlSelect SqlSelect() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    SqlNode topN = null;
    List<SqlNode> selectList;
    SqlNode e;
    SqlNode from = null;
    SqlNode where = null;
    SqlNodeList groupBy = null;
    SqlNode having = null;
    SqlNode qualify = null;
    SqlNodeList window = null;
    final List<SqlNode> hints = new ArrayList<SqlNode>();
    final Span s;
}
{
    (
        <SELECT>
    |
        <SEL>
    )
    {
        s = span();
    }
    [
        <HINT_BEG>
        CommaSepatatedSqlHints(hints)
        <COMMENT_END>
    ]
    (
        <STREAM> {
            keywords.add(SqlSelectKeyword.STREAM.symbol(getPos()));
        }
    )?
    (
        <DISTINCT> {
            keywords.add(SqlSelectKeyword.DISTINCT.symbol(getPos()));
        }
    |   <ALL> {
            keywords.add(SqlSelectKeyword.ALL.symbol(getPos()));
        }
    |
        topN = SqlSelectTopN(getPos())
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectList()
    (
        <FROM> e = FromTable(/*enableSuffix=*/true)
        {
            if (from != null) {
                throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.illegalFrom());
            }
            from = e;
        }
    |
        e = Where()
        {
            if (where != null) {
                throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.illegalWhere());
            }
            where = e;
        }
    |
        e = GroupBy()
        {
            if (groupBy != null) {
                throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.illegalGroupBy());
            }
            groupBy = (SqlNodeList) e;
        }
    |
        e = Having()
        {
            if (having != null) {
                throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.illegalHaving());
            }
            having = e;
        }
    |
        e = Qualify()
        {
            if (qualify != null) {
                throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.illegalQualify());
            }
            qualify = e;
        }
    |
        e = Window()
        {
            if (window != null) {
                throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.illegalWindow());
            }
            window = (SqlNodeList) e;
        }
    )*
    {
        // If any other clauses are present, then FROM must also be provided.
        if (from == null && (
                where != null ||
                groupBy != null ||
                having != null ||
                qualify != null ||
                window != null)
           ) {
            throw SqlUtil.newContextException(s.pos(),
                    RESOURCE.selectMissingFrom());
        }
        return new SqlSelect(s.end(this), keywordList, topN,
            new SqlNodeList(selectList, Span.of(selectList).pos()), from, where,
            groupBy, having, qualify, window, /*orderBy=*/ null,
            /*offset=*/ null, /*fetch=*/ null,
            new SqlNodeList(hints, getPos()));
    }
}

SqlNode FromTable(boolean enableSuffix) :
{
    SqlNode fromTable;
}
{
    (
        fromTable = NonQueryTable()
    |
        <LPAREN>
        (
            fromTable = FromTable(/*enableSuffix=*/true)
        |
            fromTable = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        )
        <RPAREN>
    )
    fromTable = TableAlias(fromTable)
    { if (!enableSuffix) { return fromTable; } }
    fromTable = FromSuffix(fromTable)
    { return fromTable; }
}

SqlNode FromSuffix(SqlNode e) :
{
    SqlNode e2, condition;
    SqlLiteral natural, joinType, joinConditionType;
    SqlNodeList list;
    SqlParserPos pos;
}
{
    (
        LOOKAHEAD(2)
        (
            // Decide whether to read a JOIN clause or a comma, or to quit
            //having seen a single entry FROM clause like 'FROM emps'. See
            //comments elsewhere regarding <COMMA> lookahead.
            //
            // And LOOKAHEAD(3) is needed here rather than a LOOKAHEAD(2).
            // Because currently JavaCC calculates minimum lookahead count
            // incorrectly for choice that contains zero size child. For
            // instance, with the generated code, "LOOKAHEAD(2, Natural(),
            // JoinType())" returns true immediately if it sees a single
            // "<CROSS>" token. Where we expect the lookahead succeeds after
            // "<CROSS> <APPLY>".
            //
            // For more information about the issue, see
            // https://github.com/javacc/javacc/issues/86
            LOOKAHEAD(3)
            e = JoinClause(e)
        |
            // NOTE jvs 6-Feb-2004:  See comments at top of file for why
            // hint is necessary here.  I had to use this special semantic
            // lookahead form to get JavaCC to shut up, which makes
            // me even more uneasy.
            //LOOKAHEAD({true})
            <COMMA> { joinType = JoinType.COMMA.symbol(getPos()); }
            e2 = TableRef() {
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false,
                        joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <CROSS> { joinType = JoinType.CROSS.symbol(getPos()); } <APPLY>
            e2 = TableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(),
                        RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false,
                        joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <OUTER> { joinType = JoinType.LEFT.symbol(getPos()); } <APPLY>
            e2 = TableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(),
                        RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false,
                        joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createBoolean(true,
                        joinType.getParserPosition()));
            }
        )
    )*
    {
        return e;
    }
}

SqlNode NonQueryTable() :
{
    SqlNode tableRef;
    final SqlNode over;
    final SqlNode snapshot;
    final SqlNode match;
    SqlNodeList extendList = null;
    SqlUnnestOperator unnestOp = SqlStdOperatorTable.UNNEST;
    final Span s, s2;
    SqlNodeList args;
}
{
    (
        LOOKAHEAD(2)
        tableRef = TableRefWithHintsOpt()
        [
            [ <EXTEND> ]
            extendList = ExtendList() {
                tableRef = extend(tableRef, extendList);
            }
        ]
        over = TableOverOpt() {
            if (over != null) {
                tableRef = SqlStdOperatorTable.OVER.createCall(
                    getPos(), tableRef, over);
            }
        }
        [
            tableRef = Snapshot(tableRef)
        ]
        [
            tableRef = MatchRecognize(tableRef)
        ]
    |
        <UNNEST> { s = span(); }
        args = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_SUB_QUERY)
        [
            <WITH> <ORDINALITY> {
                unnestOp = SqlStdOperatorTable.UNNEST_WITH_ORDINALITY;
            }
        ]
        {
            tableRef = unnestOp.createCall(s.end(this), args.toArray());
        }
    |
        <TABLE> { s = span(); } <LPAREN>
        tableRef = TableFunctionCall(s.pos())
        <RPAREN>
    |
        tableRef = ExtendedTableRef()
    )
    { return tableRef; }
}

SqlNode TableAlias(SqlNode tableRef) :
{
    final SqlIdentifier alias;
    SqlNodeList columnAliasList = null;
}
{
    [
        [ <AS> ] alias = SimpleIdentifier()
        [ columnAliasList = ParenthesizedSimpleIdentifierList() ]
        {
            if (columnAliasList == null) {
                tableRef = SqlStdOperatorTable.AS.createCall(
                    Span.of(tableRef).end(this), tableRef, alias);
            } else {
                List<SqlNode> idList = new ArrayList<SqlNode>();
                idList.add(tableRef);
                idList.add(alias);
                idList.addAll(columnAliasList.getList());
                tableRef = SqlStdOperatorTable.AS.createCall(
                    Span.of(tableRef).end(this), idList);
            }
        }
    ]
    {
        return tableRef;
    }
}

/** Matches "LEFT JOIN t ON ...", "RIGHT JOIN t USING ...", "JOIN t". */
SqlNode JoinClause(SqlNode e) :
{
    final SqlNode e2, condition;
    final SqlLiteral natural, joinType, joinConditionType;
    final SqlNodeList list;
}
{
    natural = Natural()
    joinType = JoinType()
    e2 = FromTable(/*enableSuffix=*/false)
    (
        <ON> {
            joinConditionType = JoinConditionType.ON.symbol(getPos());
        }
        condition = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            return new SqlJoin(joinType.getParserPosition(),
                e,
                natural,
                joinType,
                e2,
                joinConditionType,
                condition);
        }
    |
        <USING> {
            joinConditionType = JoinConditionType.USING.symbol(getPos());
        }
        list = ParenthesizedSimpleIdentifierList() {
            return new SqlJoin(joinType.getParserPosition(),
                e,
                natural,
                joinType,
                e2,
                joinConditionType,
                new SqlNodeList(list.getList(),
                Span.of(joinConditionType).end(this)));
        }
    |
        {
            return new SqlJoin(joinType.getParserPosition(),
                e,
                natural,
                joinType,
                e2,
                JoinConditionType.NONE.symbol(joinType.getParserPosition()),
                null);
        }
    )
}
/**
 * Parses the QUALIFY clause for SELECT.
 */
SqlNode Qualify() :
{
    SqlNode e;
}
{
    <QUALIFY> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { return e; }
}

/**
 * Parses an INSERT statement.
 */
SqlNode SqlInsert() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    SqlNode table;
    SqlNodeList extendList = null;
    SqlNode source;
    SqlNodeList columnList = null;
    final Span s;
}
{
    (
        <INSERT>
    |
        <INS>
    |
        <UPSERT> { keywords.add(SqlInsertKeyword.UPSERT.symbol(getPos())); }
    )
    {
        s = span();
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    [ <INTO> ]
    table = TableRefWithHintsOpt()
    [
        LOOKAHEAD(5)
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [
        LOOKAHEAD(2)
        { final Pair<SqlNodeList, SqlNodeList> p; }
        p = ParenthesizedCompoundIdentifierList() {
            if (p.right.size() > 0) {
                table = extend(table, p.right);
            }
            if (p.left.size() > 0) {
                columnList = p.left;
            }
        }
    ]
    (
        LOOKAHEAD( SqlInsertWithOptionalValuesKeyword() )
        source = SqlInsertWithOptionalValuesKeyword()
    |
        source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return new SqlInsert(s.end(source), keywordList, table, source,
            columnList);
    }
}

/**
 * Parses a DELETE statement.
 */
SqlNode SqlDelete() :
{
    SqlIdentifier deleteTableName = null;
    SqlNode table;
    final SqlNodeList tables;
    SqlNode alias;
    final SqlNodeList aliases;
    final SqlNode condition;
    final Span s;
}
{
    (
        <DELETE>
    |
        <DEL>
    )
    {
        s = span();
        tables = new SqlNodeList(s.pos());
        aliases = new SqlNodeList(s.pos());
    }
    [
        // LOOKAHEAD is required for queries like "DELETE FOO" since "FOO" in
        // this case is supposed to be "table" not "deleteTable".
        LOOKAHEAD( CompoundIdentifier() [ <FROM> ] TableRefWithHintsOpt() )
        deleteTableName = CompoundIdentifier()
    ]
    [ <FROM> ]
    table = TableRefWithHintsOpt() { tables.add(table); }
    (
        [ <AS> ] alias = SimpleIdentifier() { aliases.add(alias); }
    |
        { aliases.add(null); }
    )
    (
        <COMMA>
        table = TableRefWithHintsOpt() { tables.add(table); }
        (
            [ <AS> ] alias = SimpleIdentifier() { aliases.add(alias); }
        |
            { aliases.add(null); }
        )
    )*
    condition = WhereOpt()
    {
        return new SqlDelete(s.end(this), deleteTableName, tables, aliases,
            condition, /*sourceSelect=*/null);
    }
}

/**
 * Parses an UPDATE statement.
 */
SqlNode SqlUpdate() :
{
    SqlNode table;
    SqlNodeList sourceTables = null;
    SqlNodeList extendList = null;
    SqlIdentifier alias = null;
    SqlNode condition;
    SqlNodeList sourceExpressionList;
    SqlNodeList targetColumnList;
    SqlIdentifier id;
    SqlNode exp;
    final Span s;
    SqlNode e;
}
{
    (
        <UPDATE>
    |
        <UPD>
    )
    { s = span(); }
    table = TableRefWithHintsOpt() {
        targetColumnList = new SqlNodeList(s.pos());
        sourceExpressionList = new SqlNodeList(s.pos());
    }
    [
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [ [ <AS> ] alias = SimpleIdentifier() ]
    [
        (
            <FROM> {
                sourceTables = new SqlNodeList(s.pos());
            }
            e = TableRef() {
                sourceTables.add(e);
            }
        )
        (
            <COMMA>
            e = TableRef() {
                sourceTables.add(e);
            }
        )*
    ]
    /* CompoundIdentifier() can read statements like FOO.X, SimpleIdentifier()
       is unable to do this
    */
    <SET> id = CompoundIdentifier() {
        targetColumnList.add(id);
    }
    <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        // TODO:  support DEFAULT also
        sourceExpressionList.add(exp);
    }
    (
        <COMMA>
        id = CompoundIdentifier()
        {
            targetColumnList.add(id);
        }
        <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
            sourceExpressionList.add(exp);
        }
    )*
    condition = WhereOpt()
    {
        return new SqlUpdate(s.addAll(targetColumnList)
            .addAll(sourceExpressionList).addIf(condition).pos(), table,
            targetColumnList, sourceExpressionList, condition, null, alias,
            sourceTables);
    }
}

/**
 * Parses a prefix row operator like NOT.
 */
SqlPrefixOperator PrefixRowOperator() :
{}
{
    (
        <PLUS> { return SqlStdOperatorTable.UNARY_PLUS; }
    |
        <MINUS> { return SqlStdOperatorTable.UNARY_MINUS; }
    |
        <NOT> { return SqlStdOperatorTable.NOT; }
    |
        <CARET> { return SqlStdOperatorTable.CARET_NEGATION; }
    |
        <EXISTS> { return SqlStdOperatorTable.EXISTS; }
    )
}

/**
 * Parses a binary row operator like AND.
 */
SqlBinaryOperator BinaryRowOperator() :
{
    SqlBinaryOperator op;
}
{
    // <IN> and <LIKE> are handled as special cases
    (
        ( <EQ> | ( <CARET> | <NOT> ) <NE> ) {
            return SqlStdOperatorTable.EQUALS;
        }
    |
        ( <GT> | ( <CARET> | <NOT> ) <LE> ) {
            return SqlStdOperatorTable.GREATER_THAN;
        }
    |
        ( <LT> | ( <CARET> | <NOT> ) <GE> ) {
            return SqlStdOperatorTable.LESS_THAN;
        }
    |
        ( <LE> | ( <CARET> | <NOT> ) <GT> ) {
            return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        }
    |
        ( <GE> | ( <CARET> | <NOT> ) <LT> ) {
            return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
        }
    |
        ( <NE> | ( <CARET> | <NOT> ) <EQ> )
        {
            return SqlStdOperatorTable.NOT_EQUALS;
        }
    |
        <NE2> {
            if (!this.conformance.isBangEqualAllowed()) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.bangEqualNotAllowed());
            }
            return SqlStdOperatorTable.NOT_EQUALS;
        }
    |
        <PLUS> { return SqlStdOperatorTable.PLUS; }
    |
        <MINUS> { return SqlStdOperatorTable.MINUS; }
    |
        <STAR> { return SqlStdOperatorTable.MULTIPLY; }
    |
        <SLASH> { return SqlStdOperatorTable.DIVIDE; }
    |
        <PERCENT_REMAINDER> {
            if (!this.conformance.isPercentRemainderAllowed()) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.percentRemainderNotAllowed());
            }
            return SqlStdOperatorTable.PERCENT_REMAINDER;
        }
    |
        ( <CONCAT> | <CONCAT_BROKEN_BAR> )
        { return SqlStdOperatorTable.CONCAT; }
    |
        <AND> { return SqlStdOperatorTable.AND; }
    |
        <OR> { return SqlStdOperatorTable.OR; }
    |
        LOOKAHEAD(2) <IS> <DISTINCT> <FROM> {
            return SqlStdOperatorTable.IS_DISTINCT_FROM;
        }
    |
        <IS> <NOT> <DISTINCT> <FROM> {
            return SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
        }
    |
        <MEMBER> <OF> { return SqlStdOperatorTable.MEMBER_OF; }
    |
        LOOKAHEAD(2) <SUBMULTISET> <OF> {
            return SqlStdOperatorTable.SUBMULTISET_OF;
        }
    |
        <NOT> <SUBMULTISET> <OF> {
            return SqlStdOperatorTable.NOT_SUBMULTISET_OF;
        }
    |
        <CONTAINS> { return SqlStdOperatorTable.CONTAINS; }
    |
        <OVERLAPS> { return SqlStdOperatorTable.OVERLAPS; }
    |
        <EQUALS> { return SqlStdOperatorTable.PERIOD_EQUALS; }
    |
        <PRECEDES> { return SqlStdOperatorTable.PRECEDES; }
    |
        <SUCCEEDS> { return SqlStdOperatorTable.SUCCEEDS; }
    |
        LOOKAHEAD(2) <IMMEDIATELY> <PRECEDES> {
            return SqlStdOperatorTable.IMMEDIATELY_PRECEDES;
        }
    |
        <IMMEDIATELY> <SUCCEEDS> {
            return SqlStdOperatorTable.IMMEDIATELY_SUCCEEDS;
        }
    |
        op = BinaryMultisetOperator() { return op; }
    )
}

/**
 * Parses a binary row expression, or a parenthesized expression of any
 * kind.
 *
 * <p>The result is as a flat list of operators and operands. The top-level
 * call to get an expression should call {@link #Expression}, but lower-level
 * calls should call this, to give the parser the opportunity to associate
 * operator calls.
 *
 * <p>For example 'a = b like c = d' should come out '((a = b) like c) = d'
 * because LIKE and '=' have the same precedence, but tends to come out as '(a
 * = b) like (c = d)' because (a = b) and (c = d) are parsed as separate
 * expressions.
 */
List<Object> Expression2(ExprContext exprContext) :
{
    final List<Object> list = new ArrayList();
    List<Object> list2;
    final List<Object> list3 = new ArrayList();
    SqlNodeList nodeList;
    SqlNode e;
    SqlOperator op;
    SqlIdentifier p;
    final Span s = span();
}
{
    Expression2b(exprContext, list)
    (
        LOOKAHEAD(2)
        (
            LOOKAHEAD(2)
            (
                // Special case for "IN", because RHS of "IN" is the only place
                // that an expression-list is allowed ("exp IN (exp1, exp2)").
                LOOKAHEAD(3) {
                    checkNonQueryExpression(exprContext);
                }
                (
                    [ <IS> ] ( <NOT> | <CARET> ) <IN>
                    { op = SqlStdOperatorTable.NOT_IN; }
                |
                    [ <IS> ] <IN> { op = SqlStdOperatorTable.IN; }
                |
                    { final SqlKind k; }
                    k = comp()
                    (
                        <SOME> { op = SqlStdOperatorTable.some(k); }
                    |
                        <ANY> { op = SqlStdOperatorTable.some(k); }
                    |
                        <ALL> { op = SqlStdOperatorTable.all(k); }
                    )
                )
                { s.clear().add(this); }
                nodeList = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_NONCURSOR)
                {
                    list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
                    s.add(nodeList);
                    // special case for stuff like IN (s1 UNION s2)
                    if (nodeList.size() == 1) {
                        SqlNode item = nodeList.get(0);
                        if (item.isA(SqlKind.QUERY)) {
                            list.add(item);
                        } else {
                            list.add(nodeList);
                        }
                    } else {
                        list.add(nodeList);
                    }
                }
            |
                LOOKAHEAD(2) {
                    checkNonQueryExpression(exprContext);
                }
                (
                    <NOT> <BETWEEN> {
                        op = SqlStdOperatorTable.NOT_BETWEEN;
                        s.clear().add(this);
                    }
                    [
                        <SYMMETRIC> { op = SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN; }
                    |
                        <ASYMMETRIC>
                    ]
                |
                    <BETWEEN>
                    {
                        op = SqlStdOperatorTable.BETWEEN;
                        s.clear().add(this);
                    }
                    [
                        <SYMMETRIC> { op = SqlStdOperatorTable.SYMMETRIC_BETWEEN; }
                    |
                        <ASYMMETRIC>
                    ]
                )
                Expression2b(ExprContext.ACCEPT_SUB_QUERY, list3) {
                    list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
                    list.addAll(list3);
                    list3.clear();
                }
            |
                LOOKAHEAD(2) {
                    checkNonQueryExpression(exprContext);
                    s.clear().add(this);
                }
                (
                    LOOKAHEAD(3)
                    LikeAnyAllSome(list, s)
                |
                    (
                        (
                            ( <NOT> | <CARET> )
                            (
                                <LIKE> { op = SqlStdOperatorTable.NOT_LIKE; }
                            |
                                <SIMILAR> <TO> { op = SqlStdOperatorTable.NOT_SIMILAR_TO; }
                            )
                        |
                            <LIKE> { op = SqlStdOperatorTable.LIKE; }
                        |
                            <SIMILAR> <TO> { op = SqlStdOperatorTable.SIMILAR_TO; }
                        )
                    |
                        <NEGATE> <TILDE> { op = SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE; }
                        [ <STAR> { op = SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE; } ]
                    |
                        <TILDE> { op = SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE; }
                        [ <STAR> { op = SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE; } ]
                    )
                    list2 = Expression2(ExprContext.ACCEPT_SUB_QUERY) {
                        list.add(new SqlParserUtil.ToTreeListItem(op, s.pos()));
                        list.addAll(list2);
                    }
                )
                [
                    LOOKAHEAD(2)
                    <ESCAPE> e = Expression3(ExprContext.ACCEPT_SUB_QUERY) {
                        s.clear().add(this);
                        list.add(
                            new SqlParserUtil.ToTreeListItem(
                                SqlStdOperatorTable.ESCAPE, s.pos()));
                        list.add(e);
                    }
                ]
            |
                LOOKAHEAD(3) op = BinaryRowOperator() {
                    checkNonQueryExpression(exprContext);
                    list.add(new SqlParserUtil.ToTreeListItem(op, getPos()));
                }
                Expression2b(ExprContext.ACCEPT_SUB_QUERY, list)
            |
                <LBRACKET>
                e = Expression(ExprContext.ACCEPT_SUB_QUERY)
                <RBRACKET> {
                    list.add(
                        new SqlParserUtil.ToTreeListItem(
                            SqlStdOperatorTable.ITEM, getPos()));
                    list.add(e);
                }
                (
                    LOOKAHEAD(2) <DOT>
                    p = SimpleIdentifier() {
                        list.add(
                            new SqlParserUtil.ToTreeListItem(
                                SqlStdOperatorTable.DOT, getPos()));
                        list.add(p);
                    }
                )*
            |
                {
                    checkNonQueryExpression(exprContext);
                }
                op = PostfixRowOperator() {
                    list.add(new SqlParserUtil.ToTreeListItem(op, getPos()));
                }
            )
        )+
        {
            return list;
        }
    |
        {
            return list;
        }
    )
}

/**
 * Parses an atomic row expression.
 */
SqlNode AtomicRowExpression() :
{
    final SqlNode e;
}
{
    (
        LOOKAHEAD( DateTimeTerm() )
        e = DateTimeTerm()
    |
        e = SqlHostVariable()
    |
        e = Literal()
    |
        e = DynamicParam()
    |
        LOOKAHEAD(2)
        e = BuiltinFunctionCall()
    |
        e = JdbcFunctionCall()
    |
        e = MultisetConstructor()
    |
        e = ArrayConstructor()
    |
        LOOKAHEAD(3)
        e = MapConstructor()
    |
        e = PeriodConstructor()
    |
        // NOTE jvs 18-Jan-2005:  use syntactic lookahead to discriminate
        // compound identifiers from function calls in which the function
        // name is a compound identifier
        LOOKAHEAD( [<SPECIFIC>] FunctionName() <LPAREN>)
        e = NamedFunctionCall()
    |
        e = ContextVariable()
    |
        e = CompoundIdentifier()
    |
        e = NewSpecification()
    |
        e = CaseExpression()
    |
        e = SequenceExpression()
    )
    { return e; }
}

/**
 * Parses a CREATE statement.
 */
SqlCreate SqlCreate() :
{
    final SqlCreate create;
}
{
    (
        LOOKAHEAD(4)
        create = SqlCreateForeignSchema()
    |
        LOOKAHEAD(4)
        create = SqlCreateMacro()
    |
        LOOKAHEAD(4)
        create = SqlCreateMaterializedView()
    |
        LOOKAHEAD(4)
        create = SqlCreateSchema()
    |
        LOOKAHEAD(4)
        create = SqlCreateTable()
    |
        LOOKAHEAD(4)
        create = SqlCreateType()
    |
        LOOKAHEAD(4)
        create = SqlCreateView()
    |
        LOOKAHEAD(4)
        create = SqlCreateFunctionSqlForm()
    |
        LOOKAHEAD(4)
        create = SqlCreateJoinIndex()
    |
        LOOKAHEAD(4)
        create = SqlCreateProcedure()
    )
    {
        return create;
    }
}

/**
 * Parses a SET TIME ZONE statement
 */
SqlNode SqlSetTimeZone() :
{
    SqlNode source;
}
{
    source = SqlSetTimeZoneValue()
    {
        return source;
    }
}

SqlNode SqlUpsert() :
{
    SqlNode updateCall;
    SqlNode insertCall;
}
{
    updateCall = SqlUpdate()
    <ELSE>
    insertCall = SqlInsert()
    {
        return new SqlUpsert(span().end(this), (SqlUpdate) updateCall,
          (SqlInsert) insertCall);
    }
}

/**
 * Parses a RENAME statement
 */
SqlRename SqlRename() :
{
    SqlRename source;
}
{
    <RENAME>
    (
        source = SqlRenameMacro()
    |
        source = SqlRenameProcedure()
    |
        source = SqlRenameTable()
    )
    {
        return source;
    }
}

/**
 * Parses an EXEC statement
 */
SqlNode SqlExec() :
{
    SqlNode source;
}
{
    (
        <EXEC>
    |
        <EXECUTE>
    )
    source =  SqlExecMacro()
    {
        return source;
    }
}

/**
 * Parses a Using statement.
 */
SqlNode SqlUsing() :
{
    final Span s;
    SqlNode source;
}
{
    <USING> { s = span(); }
    source =  SqlUsingRequestModifier(s)
    {
        return source;
    }
}

/**
 * Parses a DROP statement.
 */
SqlDrop SqlDrop() :
{
    final Span s;
    boolean replace = false;
    final SqlDrop drop;
}
{
    <DROP> { s = span(); }
    (
        drop = SqlDropFunction(s, replace)
    |
        drop = SqlDropMacro(s, replace)
    |
        drop = SqlDropMaterializedView(s, replace)
    |
        drop = SqlDropProcedure(s)
    |
        drop = SqlDropSchema(s, replace)
    |
        drop = SqlDropTable(s, replace)
    |
        drop = SqlDropType(s, replace)
    |
        drop = SqlDropView(s, replace)
    )
    {
        return drop;
    }
}

/**
 * Parses a string literal. The literal may be continued onto several
 * lines.  For a simple literal, the result is an SqlLiteral.  For a continued
 * literal, the result is an SqlCall expression, which concatenates 2 or more
 * string literals; the validator reduces this.
 *
 * @see SqlLiteral#unchain(SqlNode)
 * @see SqlLiteral#stringValue(SqlNode)
 *
 * @return a literal expression
 */
SqlNode StringLiteral() :
{
    String p;
    int nfrags = 0;
    List<SqlLiteral> frags = null;
    char unicodeEscapeChar = 0;
    SqlNode hexCharLiteral;
}
{
    // A continued string literal consists of a head fragment and one or more
    // tail fragments. Since comments may occur between the fragments, and
    // comments are special tokens, each fragment is a token. But since spaces
    // or comments may not occur between the prefix and the first quote, the
    // head fragment, with any prefix, is one token.

    hexCharLiteral = SqlHexCharStringLiteral()
    { return hexCharLiteral; }
    |
    <BINARY_STRING_LITERAL>
    {
        try {
            p = SqlParserUtil.trim(token.image, "xX'");
            frags = startList(SqlLiteral.createBinaryString(p, getPos()));
            nfrags++;
        } catch (NumberFormatException ex) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.illegalBinaryString(token.image));
        }
    }
    (
        <QUOTED_STRING>
        {
            try {
                p = SqlParserUtil.trim(token.image, "'"); // no embedded quotes
                frags.add(SqlLiteral.createBinaryString(p, getPos()));
                nfrags++;
            } catch (NumberFormatException ex) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.illegalBinaryString(token.image));
            }
        }
    )*
    {
        assert (nfrags > 0);
        if (nfrags == 1) {
            return frags.get(0); // just the head fragment
        } else {
            SqlParserPos pos2 = SqlParserPos.sum(frags);
            return SqlStdOperatorTable.LITERAL_CHAIN.createCall(pos2, frags);
        }
    }
    |
    {
        String charSet = null;
    }
    (
        <PREFIXED_STRING_LITERAL>
        { charSet = SqlParserUtil.getCharacterSet(token.image); }
    |   <QUOTED_STRING>
    |   <UNICODE_STRING_LITERAL> {
            // TODO jvs 2-Feb-2009:  support the explicit specification of
            // a character set for Unicode string literals, per SQL:2003
            unicodeEscapeChar = BACKSLASH;
            charSet = "UTF16";
        }
    )
    {
        p = SqlParserUtil.parseString(token.image);
        SqlCharStringLiteral literal;
        try {
            literal = SqlLiteral.createCharString(p, charSet, getPos());
        } catch (java.nio.charset.UnsupportedCharsetException e) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.unknownCharacterSet(charSet));
        }
        frags = startList(literal);
        nfrags++;
    }
    (
        <QUOTED_STRING>
        {
            p = SqlParserUtil.parseString(token.image);
            try {
                literal = SqlLiteral.createCharString(p, charSet, getPos());
            } catch (java.nio.charset.UnsupportedCharsetException e) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.unknownCharacterSet(charSet));
            }
            frags.add(literal);
            nfrags++;
        }
    )*
    [
        <UESCAPE> <QUOTED_STRING>
        {
            if (unicodeEscapeChar == 0) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.unicodeEscapeUnexpected());
            }
            String s = SqlParserUtil.parseString(token.image);
            unicodeEscapeChar = SqlParserUtil.checkUnicodeEscapeChar(s);
        }
    ]
    {
        assert nfrags > 0;
        if (nfrags == 1) {
            // just the head fragment
            SqlLiteral lit = (SqlLiteral) frags.get(0);
            return lit.unescapeUnicode(unicodeEscapeChar);
        } else {
            SqlNode[] rands = (SqlNode[]) frags.toArray(new SqlNode[nfrags]);
            for (int i = 0; i < rands.length; ++i) {
                rands[i] = ((SqlLiteral) rands[i]).unescapeUnicode(
                    unicodeEscapeChar);
            }
            SqlParserPos pos2 = SqlParserPos.sum(rands);
            return SqlStdOperatorTable.LITERAL_CHAIN.createCall(pos2, rands);
        }
    }
}

/**
 * Parses a compound identifier.
 */
SqlIdentifier CompoundIdentifier() :
{
    final List<String> nameList = new ArrayList<String>();
    final List<SqlParserPos> posList = new ArrayList<SqlParserPos>();
    boolean star = false;
}
{
    IdentifierSegment(nameList, posList)
    [
        LOOKAHEAD(2)
        <COLON>
        IdentifierSegment(nameList, posList)
    ]
    (
        LOOKAHEAD(2)
        <DOT>
        IdentifierSegment(nameList, posList)
    )*
    (
        LOOKAHEAD(2)
        <DOT>
        <STAR> {
            star = true;
            nameList.add("");
            posList.add(getPos());
        }
    )?
    {
        SqlParserPos pos = SqlParserPos.sum(posList);
        if (star) {
            return SqlIdentifier.star(nameList, pos, posList);
        }
        return new SqlIdentifier(nameList, null, pos, posList);
    }
}

/**
 * Parses a call to a builtin function with special syntax.
 */
SqlNode BuiltinFunctionCall() :
{
    final SqlIdentifier name;
    List<SqlNode> args = null;
    SqlNode e = null;
    final Span s;
    SqlDataTypeSpec dt;
    TimeUnit interval;
    final TimeUnit unit;
    final SqlNode node;
    final boolean isTranslateChk;
    boolean isWithError = false;
    boolean allowTranslateUsingCharSet = false;
}
{
    //~ FUNCTIONS WITH SPECIAL SYNTAX ---------------------------------------
    (
        { final SqlKind castKind; }
        (
            <CAST> { castKind = SqlKind.CAST; }
        |
            <TRYCAST> { castKind = SqlKind.TRYCAST; }
        )
        { s = span(); }
        <LPAREN> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args = startList(e); }
        <AS>
        (
            ( e = AlternativeTypeConversionAttribute() { args.add(e); } )+
        |
            (
                dt = DataType() { args.add(dt); }
            |
                <INTERVAL> e = IntervalQualifier() { args.add(e); }
            )
            ( e = AlternativeTypeConversionAttribute() { args.add(e); } )*
        )
        <RPAREN> {
            return castKind == SqlKind.TRYCAST
                ? SqlStdOperatorTable.TRYCAST.createCall(s.end(this), args)
                : SqlStdOperatorTable.CAST.createCall(s.end(this), args);
        }
    |
        e = SqlExtractFromDateTime() { return e; }
    |
        <POSITION> { s = span(); }
        <LPAREN>
        // FIXME jvs 31-Aug-2006:  FRG-192:  This should be
        // Expression(ExprContext.ACCEPT_SUB_QUERY), but that doesn't work
        // because it matches the other kind of IN.
        e = AtomicRowExpression() { args = startList(e); }
        <IN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e);}
        [
            <FROM>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
        ]
        <RPAREN> {
            return SqlStdOperatorTable.POSITION.createCall(s.end(this), args);
        }
    |
        <CONVERT> { s = span(); }
        <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args = startList(e);
        }
        <USING> name = SimpleIdentifier() {
            args.add(name);
        }
        <RPAREN> {
            return SqlStdOperatorTable.CONVERT.createCall(s.end(this), args);
        }
    |
        (
            <TRANSLATE> { isTranslateChk = false; }
        |
            <TRANSLATE_CHK> { isTranslateChk = true; }
        )
        { s = span(); }
        <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args = startList(e);
        }
        (
            <USING> name = SimpleIdentifier() {
                String nameString = name.toString().toUpperCase();
                String[] charSets = new String[2];
                if (nameString.contains("_TO_")) {
                    charSets = nameString.split("_TO_");
                    if (charSets.length == 2) {
                        allowTranslateUsingCharSet = true;
                        args.add(new SqlCharacterSetToCharacterSet(charSets, getPos()));
                    }
                } else {
                    args.add(name);
                }
            }
            [
                <WITH> <ERROR> {
                    isWithError = true;
                }
            ]
            <RPAREN> {
                if (allowTranslateUsingCharSet) {
                    return new SqlTranslateUsingCharacterSet(s.end(this), args,
                        isTranslateChk, isWithError);
                }
                return isTranslateChk ?
                    SqlStdOperatorTable.TRANSLATE_CHK.createCall(s.end(this), args) :
                    SqlStdOperatorTable.TRANSLATE.createCall(s.end(this), args);
            }
        |
            (
                <COMMA> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                    args.add(e);
                }
            )*
            <RPAREN> {
                return SqlLibraryOperators.TRANSLATE3.createCall(s.end(this),
                    args);
            }
        )
    |
        <OVERLAY> { s = span(); }
        <LPAREN> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args = startList(e);
        }
        <PLACING> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args.add(e);
        }
        <FROM> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args.add(e);
        }
        [
            <FOR> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                args.add(e);
            }
        ]
        <RPAREN> {
            return SqlStdOperatorTable.OVERLAY.createCall(s.end(this), args);
        }
    |
        <FLOOR> { s = span(); }
        e = FloorCeilOptions(s, true) {
            return e;
        }
    |
        ( <CEIL> | <CEILING>) { s = span(); }
        e = FloorCeilOptions(s, false) {
            return e;
        }
    |
        (
            <SUBSTRING>
        |
            <SUBSTR>
        )
        { s = span(); }
        <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY)
        { args = startList(e); }
        ( <FROM> | <COMMA>)
        e = Expression(ExprContext.ACCEPT_SUB_QUERY)
        { args.add(e); }
        [
            (<FOR> | <COMMA>)
            e = Expression(ExprContext.ACCEPT_SUB_QUERY)
            { args.add(e); }
        ]
        <RPAREN> {
            return SqlStdOperatorTable.SUBSTRING.createCall(
                s.end(this), args);
        }
    |
        <TRIM> {
            SqlLiteral flag = null;
            SqlNode trimChars = null;
            s = span();
        }
        <LPAREN>
        [
            LOOKAHEAD(2)
            [
                <BOTH> {
                    s.add(this);
                    flag = SqlTrimFunction.Flag.BOTH.symbol(getPos());
                }
            |
                <TRAILING> {
                    s.add(this);
                    flag = SqlTrimFunction.Flag.TRAILING.symbol(getPos());
                }
            |
                <LEADING> {
                    s.add(this);
                    flag = SqlTrimFunction.Flag.LEADING.symbol(getPos());
                }
            ]
            [ trimChars = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
            (
                <FROM> {
                    if (null == flag && null == trimChars) {
                        throw SqlUtil.newContextException(getPos(),
                            RESOURCE.illegalFromEmpty());
                    }
                }
            |
                <RPAREN> {
                    // This is to handle the case of TRIM(x)
                    // (FRG-191).
                    if (flag == null) {
                        flag = SqlTrimFunction.Flag.BOTH.symbol(SqlParserPos.ZERO);
                    }
                    args = startList(flag);
                    args.add(null); // no trim chars
                    args.add(trimChars); // reinterpret trimChars as source
                    return SqlStdOperatorTable.TRIM.createCall(s.end(this),
                        args);
                }
            )
        ]
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            if (flag == null) {
                flag = SqlTrimFunction.Flag.BOTH.symbol(SqlParserPos.ZERO);
            }
            args = startList(flag);
            args.add(trimChars);
            args.add(e);
        }
        <RPAREN> {
            return SqlStdOperatorTable.TRIM.createCall(s.end(this), args);
        }
    |
        node = TimestampAddFunctionCall() { return node; }
    |
        node = TimestampDiffFunctionCall() { return node; }
    |
        node = DateFunctionCall() { return node; }
    |
        node = DateAddFunctionCall() { return node; }
    |
        node = CurrentTimestampFunction() { return node; }
    |
        node = CurrentTimeFunction() { return node; }
    |
        node = CurrentDateFunction() { return node; }
    |
        node = TimeFunctionCall() { return node; }
    |
        LOOKAHEAD(<RANK>, { getToken(3).kind != RPAREN })
        node = RankFunctionCallWithParams() { return node; }
    |
        node = MatchRecognizeFunctionCall() { return node; }
    |
        node = JsonExistsFunctionCall() { return node; }
    |
        node = JsonValueFunctionCall() { return node; }
    |
        node = JsonQueryFunctionCall() { return node; }
    |
        node = JsonObjectFunctionCall() { return node; }
    |
        node = JsonObjectAggFunctionCall() { return node; }
    |
        node = JsonArrayFunctionCall() { return node; }
    |
        node = JsonArrayAggFunctionCall() { return node; }
    |
        node = GroupByWindowingCall() { return node; }
    |
        node = CaseN() { return node; }
    |
        node = RangeN() { return node; }
    |
        node = FirstLastValue() { return node; }
    )
}

/**
 * Parses a call to a named function (could be a builtin with regular
 * syntax, or else a UDF).
 *
 * <p>NOTE: every UDF has two names: an <em>invocation name</em> and a
 * <em>specific name</em>.  Normally, function calls are resolved via overload
 * resolution and invocation names.  The SPECIFIC prefix allows overload
 * resolution to be bypassed.  Note that usage of the SPECIFIC prefix in
 * queries is non-standard; it is used internally by Farrago, e.g. in stored
 * view definitions to permanently bind references to a particular function
 * after the overload resolution performed by view creation.
 *
 * <p>TODO jvs 25-Mar-2005:  Once we have SQL-Flagger support, flag SPECIFIC
 * as non-standard.
 */
SqlNode NamedFunctionCall() :
{
    final SqlFunctionCategory funcType;
    final SqlIdentifier qualifiedName;
    final Span s;
    final List<SqlNode> args;
    SqlCall call;
    final Span filterSpan;
    final SqlNode filter;
    final SqlNode over;
    SqlLiteral quantifier = null;
    SqlNodeList orderList = null;
    final Span withinGroupSpan;
    SqlNode e;
}
{
    (
        <SPECIFIC> {
            funcType = SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION;
        }
    |
        { funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION; }
    )
    qualifiedName = FunctionName() {
        s = span();
    }
    (
        LOOKAHEAD(2) <LPAREN> <STAR> {
            args = startList(SqlIdentifier.star(getPos()));
        }
        <RPAREN>
    |
        LOOKAHEAD(2) <LPAREN> <RPAREN> {
            args = Collections.emptyList();
        }
    |
        args = FunctionParameterList(ExprContext.ACCEPT_SUB_QUERY) {
            quantifier = (SqlLiteral) args.get(0);
            args.remove(0);
        }
    )
    {
        call = createCall(qualifiedName, s.end(this), funcType, quantifier, args);
    }
    [
        call = withinGroup(call)
    ]
    [
        <FILTER> { filterSpan = span(); }
        <LPAREN>
        <WHERE>
        filter = Expression(ExprContext.ACCEPT_SUB_QUERY)
        <RPAREN> {
            call = SqlStdOperatorTable.FILTER.createCall(
                filterSpan.end(this), call, filter);
        }
    ]
    [
        <OVER>
        (
            over = SimpleIdentifier()
        |
            over = WindowSpecification()
        )
        {
            call = SqlStdOperatorTable.OVER.createCall(s.end(over), call, over);
        }
    ]
    { return call; }
}

SqlLiteral JoinType() :
{
    JoinType joinType;
}
{
    (
        <JOIN> { joinType = JoinType.INNER; }
    |
        <INNER> <JOIN> { joinType = JoinType.INNER; }
    |
        <LEFT> [ <OUTER> ] <JOIN> { joinType = JoinType.LEFT; }
    |
        <RIGHT> [ <OUTER> ] <JOIN> { joinType = JoinType.RIGHT; }
    |
        <FULL> [ <OUTER> ] <JOIN> { joinType = JoinType.FULL; }
    |
        <CROSS> <JOIN> { joinType = JoinType.CROSS; }
    )
    {
        return joinType.symbol(getPos());
    }
}

/* LITERALS */

<DEFAULT, DQID, BTID> TOKEN :
{
    /* To improve error reporting, we allow all kinds of characters,
     * not just hexits, in a binary string literal. */
    < PREFIXED_HEX_STRING_LITERAL :
    ("_" <CHARSETNAME>| "_" <CHARSETNAME> " ") <QUOTED_HEX_STRING> >
|
    < QUOTED_HEX_STRING : <QUOTE> (<HEXDIGIT>)+ <QUOTE> (("XC") | ("XCV") | ("XCF"))>
}

/**
 * Parses a unary row expression, or a parenthesized expression of any
 * kind.
 */
SqlNode Expression3(ExprContext exprContext) :
{
    final SqlNode e;
    final SqlNodeList list;
    final SqlNodeList list1;
    final SqlNodeList list2;
    final SqlOperator op;
    final Span s;
    final SqlNode inlineCall;
    Span rowSpan = null;
}
{
    LOOKAHEAD(InlineCaseSpecific())
    e = InlineCaseSpecific() { return e; }
|
    LOOKAHEAD(InlineModOperatorLiteralOrIdentifier())
    e = InlineModOperatorLiteralOrIdentifier() { return e; }
|
    LOOKAHEAD(NamedLiteralOrIdentifier())
    e = NamedLiteralOrIdentifier() { return e; }
|
    LOOKAHEAD(AlternativeTypeConversionLiteralOrIdentifier())
    e = AlternativeTypeConversionLiteralOrIdentifier() { return e; }
|
    LOOKAHEAD(2)
    e = AtomicRowExpression()
    (
        inlineCall = NamedQuery(e) { return inlineCall; }
    |
        inlineCall = AlternativeTypeConversionQuery(e) { return inlineCall; }
    |
        inlineCall = CaseSpecific(e) { return inlineCall; }
    |
        {
            checkNonQueryExpression(exprContext);
            return e;
        }
    )
|
    e = CursorExpression(exprContext) { return e; }
|
    LOOKAHEAD(3)
    <ROW> {
        s = span();
    }
    list = ParenthesizedSimpleIdentifierList() {
        if (exprContext != ExprContext.ACCEPT_ALL
            && exprContext != ExprContext.ACCEPT_CURSOR
            && !this.conformance.allowExplicitRowValueConstructor())
        {
            throw SqlUtil.newContextException(s.end(list),
                RESOURCE.illegalRowExpression());
        }
        return SqlStdOperatorTable.ROW.createCall(list);
    }
|
    [
        <ROW> { rowSpan = span(); }
    ]
    list1 = ParenthesizedQueryOrCommaList(exprContext) {
        if (rowSpan != null) {
            // interpret as row constructor
            return SqlStdOperatorTable.ROW.createCall(rowSpan.end(list1),
                list1.toArray());
        }
    }
    [
        LOOKAHEAD(2)
        /* TODO:
        (
            op = periodOperator()
            list2 = ParenthesizedQueryOrCommaList(exprContext)
            {
                if (list1.size() != 2 || list2.size() != 2) {
                    throw SqlUtil.newContextException(
                        list1.getParserPosition().plus(
                            list2.getParserPosition()),
                        RESOURCE.illegalOverlaps());
                }
                for (SqlNode node : list2) {
                    list1.add(node);
                }
                return op.createCall(
                    list1.getParserPosition().plus(list2.getParserPosition()),
                    list1.toArray());
            }
        )
    |
        */
        (
            e = IntervalQualifier()
            {
                if ((list1.size() == 1)
                    && list1.get(0) instanceof SqlCall)
                {
                    final SqlCall call = (SqlCall) list1.get(0);
                    if (call.getKind() == SqlKind.MINUS
                            && call.operandCount() == 2) {
                        List<SqlNode> list3 = startList(call.operand(0));
                        list3.add(call.operand(1));
                        list3.add(e);
                        return SqlStdOperatorTable.MINUS_DATE.createCall(
                            Span.of(list1).end(this), list3);
                     }
                }
                throw SqlUtil.newContextException(span().end(list1),
                    RESOURCE.illegalMinusDate());
            }
        )
    ]
    {
        if (list1.size() != 1) {
            // interpret as row constructor
            return SqlStdOperatorTable.ROW.createCall(span().end(list1),
                list1.toArray());
        }
    }
    (
        e = NamedQuery(list1.get(0)) { return e; }
    |
        e = AlternativeTypeConversionQuery(list1.get(0)) { return e; }
    |
        e = InlineModOperator(list1.get(0)) { return e; }
    |
        { return list1.get(0); }
    )
}

/**
 * Parses an expression for setting or resetting an option in SQL, such as QUOTED_IDENTIFIERS,
 * or explain plan level (physical/logical).
 */
SqlAlter SqlAlter() :
{
    final Span s;
    final String scope;
    final SqlAlter alterNode;
}
{
    <ALTER> { s = span(); }
    (
        scope = Scope()
    |
        { scope = null; }
    )
    (
        alterNode = SqlAlterProcedure(s, scope)
    |
        alterNode = SqlAlterTable(s, scope)
    |
        alterNode = SqlSetOption(s, scope)
    )
    {
        return alterNode;
    }
}

// Some SQL type names need special handling due to the fact that they have
// spaces in them but are not quoted.
SqlTypeNameSpec TypeName() :
{
    final SqlTypeNameSpec typeNameSpec;
    final SqlIdentifier typeName;
    final Span s = Span.of();
}
{
    (
        LOOKAHEAD(2)
        typeNameSpec = BlobDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = ByteDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = ByteIntType()
    |
        LOOKAHEAD(2)
        typeNameSpec = ClobDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = NumberDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = SqlJsonDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = SqlPeriodDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = VarbyteDataType()
    |
        LOOKAHEAD(2)
        typeNameSpec = SqlTypeName(s)
    |
        typeNameSpec = RowTypeName()
    |
        typeName = CompoundIdentifier() {
            typeNameSpec = new SqlUserDefinedTypeNameSpec(typeName, s.end(this));
        }
    )
    {
        return typeNameSpec;
    }
}

/**
* Parse datetime types: date, time, timestamp.
* Override the function in core parser.
*/
SqlTypeNameSpec DateTimeTypeName() :
{
    int precision = -1;
    final SqlTypeName typeName;
    SqlTimeZoneOption timeZoneOpt = SqlTimeZoneOption.WITHOUT_TIME_ZONE;
}
{
    <DATE> {
        typeName = SqlTypeName.DATE;
        return new SqlBasicTypeNameSpec(typeName, getPos());
    }
|
    LOOKAHEAD(2)
    <TIME>
    precision = PrecisionOpt()
    timeZoneOpt = TimeZoneOpt()
    {
        switch (timeZoneOpt) {
           case WITH_LOCAL_TIME_ZONE:
              typeName = SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
              break;
           case WITH_TIME_ZONE:
              typeName = SqlTypeName.TIME_WITH_TIME_ZONE;
              break;
           case WITHOUT_TIME_ZONE:
           default:
              typeName = SqlTypeName.TIME;
              break;
        }
        return new SqlBasicTypeNameSpec(typeName, precision, getPos());
    }
|
    <TIMESTAMP>
    precision = PrecisionOpt()
    timeZoneOpt = TimeZoneOpt()
    {
        switch (timeZoneOpt) {
        case WITH_LOCAL_TIME_ZONE:
            typeName = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            break;
        case WITH_TIME_ZONE:
            typeName = SqlTypeName.TIMESTAMP_WITH_TIME_ZONE;
            break;
        case WITHOUT_TIME_ZONE:
        default:
            typeName = SqlTypeName.TIMESTAMP;
            break;
        }
        return new SqlBasicTypeNameSpec(typeName, precision, getPos());
    }
}

SqlTimeZoneOption TimeZoneOpt() :
{
}
{
    LOOKAHEAD(3)
    <WITHOUT> <TIME> <ZONE>
    { return SqlTimeZoneOption.WITHOUT_TIME_ZONE; }
|
    LOOKAHEAD(3)
    <WITH> <LOCAL> <TIME> <ZONE>
    { return SqlTimeZoneOption.WITH_LOCAL_TIME_ZONE; }
|
    <WITH> <TIME> <ZONE>
    { return SqlTimeZoneOption.WITH_TIME_ZONE; }
|
    { return SqlTimeZoneOption.WITHOUT_TIME_ZONE;}
}

SqlCaseN CaseN() :
{
    final SqlNodeList nodes = new SqlNodeList(getPos());
    SqlNode e;
    SqlPartitionByNoneUnknown extraPartitionOption = null;
}
{
    <CASE_N>
    <LPAREN>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
        nodes.add(e);
    }
    (
        <COMMA>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
            nodes.add(e);
        }
    )*
    [
        <COMMA>
        (
            LOOKAHEAD(3)
            <NO> <CASE> <OR> <UNKNOWN>
            {
                extraPartitionOption =
                    SqlPartitionByNoneUnknown.NONE_OR_UNKNOWN;
            }
        |
            LOOKAHEAD(3)
            <NO> <CASE> <COMMA> <UNKNOWN>
            {
                extraPartitionOption =
                    SqlPartitionByNoneUnknown.NONE_COMMA_UNKNOWN;
            }
        |
            <NO> <CASE>
            { extraPartitionOption = SqlPartitionByNoneUnknown.NONE; }
        |
            <UNKNOWN>
            { extraPartitionOption = SqlPartitionByNoneUnknown.UNKNOWN; }
        )
    ]
    <RPAREN>
    {
        return new SqlCaseN(getPos(), nodes, extraPartitionOption);
    }
}

SqlRangeN RangeN() :
{
    final SqlNode testExpression;
    SqlNode range;
    SqlNode startLiteral = null;
    boolean startAsterisk = false;
    SqlNode endLiteral = null;
    boolean endAsterisk = false;
    SqlNode eachSizeLiteral = null;
    final SqlNodeList rangeList = new SqlNodeList(getPos());
    SqlPartitionByNoneUnknown extraPartitionOption = null;
}
{
    <RANGE_N>
    <LPAREN>
    testExpression = AtomicRowExpression()
    <BETWEEN>
    (
        <STAR> { startAsterisk = true; }
        [
            <AND>
            (
                <STAR> { endAsterisk = true; }
            |
                endLiteral = Literal()
            )
        ]
    |
        startLiteral = Literal()
        [
            <AND>
            (
                <STAR> { endAsterisk = true; }
            |
                endLiteral = Literal()
            )
        ]
        [
            <EACH>
            eachSizeLiteral = Literal()
        ]
    )
    {
        rangeList.add(new SqlRangeNStartEnd(getPos(), startLiteral, endLiteral,
            eachSizeLiteral, startAsterisk, endAsterisk));
    }
    (
        <COMMA>
        range = RangeNStartEnd()
        { rangeList.add(range); }
    )*
    [
        <COMMA>
        (
            LOOKAHEAD(3)
            <NO> <RANGE> <OR> <UNKNOWN>
            {
                extraPartitionOption =
                    SqlPartitionByNoneUnknown.NONE_OR_UNKNOWN;
            }
        |
            LOOKAHEAD(3)
            <NO> <RANGE> <COMMA> <UNKNOWN>
            {
                extraPartitionOption =
                    SqlPartitionByNoneUnknown.NONE_COMMA_UNKNOWN;
            }
        |
            <NO> <RANGE>
            { extraPartitionOption = SqlPartitionByNoneUnknown.NONE; }
        |
            <UNKNOWN>
            { extraPartitionOption = SqlPartitionByNoneUnknown.UNKNOWN; }
        )
    ]
    <RPAREN>
    {
        return new SqlRangeN(getPos(), testExpression, rangeList,
            extraPartitionOption);
    }
}

SqlRangeNStartEnd RangeNStartEnd() :
{
    final SqlNode startLiteral;
    SqlNode endLiteral = null;
    boolean endAsterisk = false;
    SqlNode eachSizeLiteral = null;
}
{
    startLiteral = Literal()
    [
        <AND>
        (
            <STAR> { endAsterisk = true; }
        |
            endLiteral = Literal()
        )
    ]
    [
        <EACH>
        eachSizeLiteral = Literal()
    ]
    {
        return new SqlRangeNStartEnd(getPos(), startLiteral, endLiteral,
            eachSizeLiteral, false, endAsterisk);
    }
}

SqlCreateProcedure SqlCreateProcedure() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final SqlIdentifier procedureName;
    final List<SqlCreateProcedureParameter> parameters =
        new ArrayList<SqlCreateProcedureParameter>();
    final CreateProcedureDataAccess access;
    SqlLiteral numResultSets = null;
    final CreateProcedureSecurity security;
    final SqlNode statement;
    SqlCreateProcedureParameter parameter;
}
{
    (
        <CREATE> { createSpecifier = SqlCreateSpecifier.CREATE; }
    |
        <REPLACE> { createSpecifier = SqlCreateSpecifier.REPLACE; }
    )
    { s = span(); }
    <PROCEDURE>
    procedureName = CompoundIdentifier()
    <LPAREN>
    [
        parameter = SqlCreateProcedureParameter() {
            parameters.add(parameter);
        }
        (
            <COMMA>
            parameter = SqlCreateProcedureParameter() {
                parameters.add(parameter);
            }
        )*
    ]
    <RPAREN>
    (
        <CONTAINS> <SQL> { access = CreateProcedureDataAccess.CONTAINS_SQL; }
    |
        <MODIFIES> <SQL> <DATA> {
            access = CreateProcedureDataAccess.MODIFIES_SQL_DATA;
        }
    |
        <READS> <SQL> <DATA> {
            access = CreateProcedureDataAccess.READS_SQL_DATA;
        }
    |
        { access = CreateProcedureDataAccess.UNSPECIFIED; }
    )
    [
        <DYNAMIC> <RESULT> <SETS>
        numResultSets = UnsignedNumericLiteral() {
            int numericNumResultSets = numResultSets.getValueAs(Integer.class);
            if (numericNumResultSets < 0 || numericNumResultSets > 15) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.numberLiteralOutOfRange(
                    String.valueOf(numericNumResultSets)));
            }
        }
    ]
    (
        <SQL> <SECURITY>
        (
            <CREATOR> { security = CreateProcedureSecurity.CREATOR; }
        |
            <DEFINER> { security = CreateProcedureSecurity.DEFINER; }
        |
            <INVOKER> { security = CreateProcedureSecurity.INVOKER; }
        |
            <OWNER> { security = CreateProcedureSecurity.OWNER; }
        )
    |
        { security = CreateProcedureSecurity.UNSPECIFIED; }
    )
    statement = CreateProcedureStmt()
    {
        return new SqlCreateProcedure(s.end(this), createSpecifier,
            procedureName, parameters, access, numResultSets, security,
            statement);
    }
}

SqlNode CreateProcedureStmt() :
{
    final SqlNode e;
}
{
    (
        e = ConditionalStmt()
    |
        // LOOKAHEAD ensures statements such as UPDATE table and EXECUTE macro
        // do not get parsed by CursorStmt() when they should be parsed by
        // SqlStmt().
        LOOKAHEAD(CursorStmt() <SEMICOLON>)
        e = CursorStmt()
    |
        e = DiagnosticStmt()
    |
        e = IterateStmt()
    |
        // This lookahead ensures parser chooses the right path when facing
        // begin label.
        LOOKAHEAD(3)
        e = IterationStmt()
    |
        e = LeaveStmt()
    |
        e = SetStmt()
    |
        e = SqlBeginEndCall()
    |
        e = SqlBeginRequestCall()
    |
        e = SqlStmt()
    )
    { return e; }
}

void CreateProcedureStmtList(SqlStatementList statements) :
{
    SqlNode e;
}
{
    (
        e = CreateProcedureStmt() <SEMICOLON> {
            statements.add(e);
        }
    )+
}

SqlCreateProcedureParameter SqlCreateProcedureParameter() :
{
    final CreateProcedureParameterType parameterType;
    final SqlIdentifier name;
    final SqlDataTypeSpec dataType;
}
{
    (
        <OUT> { parameterType =  CreateProcedureParameterType.OUT; }
    |
        <INOUT> { parameterType =  CreateProcedureParameterType.INOUT; }
    |
        [ <IN> ] { parameterType =  CreateProcedureParameterType.IN; }
    )
    name = SimpleIdentifier()
    dataType = DataType()
    { return new SqlCreateProcedureParameter(parameterType, name, dataType); }
}

SqlAlter SqlAlterProcedure(Span s, String scope) :
{
     final SqlIdentifier procedureName;
     boolean languageSql = false;
     final List<AlterProcedureWithOption> withOptions =
        new ArrayList<AlterProcedureWithOption>();
     boolean local = false;
     boolean isTimeZoneNegative = false;
     String timeZoneString = null;
     AlterProcedureWithOption option;
}
{
    <PROCEDURE>
    procedureName = CompoundIdentifier()
    [
        <LANGUAGE> <SQL> { languageSql = true;}
    ]
    <COMPILE>
    [
        <WITH> option = AlterProcedureWithOption() {
            withOptions.add(option);
        }
        (
            <COMMA> option = AlterProcedureWithOption() {
                withOptions.add(option);
            }
        )*
    ]
    [
        <AT> <TIME> <ZONE>
        (
            <LOCAL> { local = true; }
        |
            (
                <MINUS> { isTimeZoneNegative = true; }
            |
                [ <PLUS> ]
            )
            <QUOTED_STRING> { timeZoneString = token.image; }
        )
    ]
    {
        return new SqlAlterProcedure(getPos(), scope, procedureName,
            languageSql, withOptions, local, isTimeZoneNegative,
            timeZoneString);
    }
}

AlterProcedureWithOption AlterProcedureWithOption() :
{
}
{
    (
        <SPL> { return AlterProcedureWithOption.SPL; }
    |
        <WARNING> { return AlterProcedureWithOption.WARNING; }
    |
        <NO> <SPL> { return AlterProcedureWithOption.NO_SPL; }
    |
        <NO> <WARNING> { return AlterProcedureWithOption.NO_WARNING; }
    )
}

SqlBeginEndCall SqlBeginEndCall() :
{
    SqlIdentifier beginLabel = null;
    SqlIdentifier endLabel = null;
    final SqlStatementList statements = new SqlStatementList(getPos());
    final Span s = Span.of();
    SqlNode e;
}
{
    [ beginLabel = SimpleIdentifier() <COLON> ]
    <BEGIN>
    (
        // LOOKAHEAD ensures statement should not be parsed by
        // SqlDeclareCursor() instead.
        LOOKAHEAD(LocalDeclaration())
        e = LocalDeclaration() { statements.add(e); }
    )*
    (
        // LOOKAHEAD ensures statement should not be parsed by
        // SqlDeclareHandlerStmt() instead.
        LOOKAHEAD(3)
        e = SqlDeclareCursor() { statements.add(e); }
    )*
    ( e = SqlDeclareHandlerStmt() { statements.add(e); } )*
    [ CreateProcedureStmtList(statements) ]
    <END>
    [ endLabel = SimpleIdentifier() ]
    {
        return new SqlBeginEndCall(s.end(this), beginLabel, endLabel,
            statements);
    }
}

SqlCall LocalDeclaration() :
{
    final SqlCall e;
}
{
    (
        LOOKAHEAD(3)
        e = SqlDeclareConditionStmt()
    |
        e = SqlDeclareVariable()
    )
    <SEMICOLON>
    { return e; }
}

SqlDeclareVariable SqlDeclareVariable() :
{
    final SqlNodeList variableNames = new SqlNodeList(getPos());
    final SqlDataTypeSpec dataType;
    final SqlNode defaultValue;
    final Span s = Span.of();
    SqlIdentifier variableName;
}
{
    <DECLARE>
    variableName = SimpleIdentifier() { variableNames.add(variableName); }
    (
        <COMMA> variableName = SimpleIdentifier() {
            variableNames.add(variableName);
        }
    )*
    dataType = DataType()
    (
        <DEFAULT_>
        (
            defaultValue = Literal()
        |
            <NULL> { defaultValue = SqlLiteral.createNull(getPos()); }
        )
    |
        { defaultValue = null; }
    )
    {
        return new SqlDeclareVariable(s.end(this), variableNames, dataType,
            defaultValue);
    }
}

SqlDeclareConditionStmt SqlDeclareConditionStmt() :
{
    final SqlIdentifier conditionName;
    final SqlNode stateCode;
    final Span s = Span.of();
}
{
    <DECLARE>
    conditionName = SimpleIdentifier()
    <CONDITION>
    (
        <FOR> stateCode = StringLiteral()
    |
        { stateCode = null; }
    )
    { return new SqlDeclareConditionStmt(s.end(this), conditionName, stateCode); }
}

SqlDrop SqlDropProcedure(Span s) :
{
    final SqlIdentifier procedureName;
}
{
    <PROCEDURE> procedureName = CompoundIdentifier() {
        return new SqlDropProcedure(s.end(this), procedureName);
    }
}

/**
 * Parses a HELP statement.
 */
SqlHelp SqlHelp() :
{
    final SqlHelp help;
    final Span s;
}
{
    <HELP> { s = span(); }
    (
        help = SqlHelpProcedure(s)
    )
    { return help; }
}

SqlHelpProcedure SqlHelpProcedure(Span s) :
{
    final SqlIdentifier procedureName;
    boolean attributes = false;
}
{
    <PROCEDURE> procedureName = CompoundIdentifier()
    [
        ( <ATTRIBUTES> | <ATTR> | <ATTRS> ) {
            attributes = true;
        }
    ]
    { return new SqlHelpProcedure(s.end(this), procedureName, attributes); }
}

SqlRenameProcedure SqlRenameProcedure() :
{
    final SqlIdentifier oldProcedure;
    final SqlIdentifier newProcedure;
}
{
    <PROCEDURE>
    oldProcedure = CompoundIdentifier()
    (
        <TO>
    |
        <AS>
    )
    newProcedure = CompoundIdentifier()
    {
        return new SqlRenameProcedure(getPos(), oldProcedure, newProcedure);
    }
}

// Semicolon is optional after the last statement.
SqlBeginRequestCall SqlBeginRequestCall() :
{
    final SqlStatementList statements = new SqlStatementList(getPos());
    final Span s = Span.of();
    SqlNode e;
}
{
    <BEGIN> <REQUEST>
    e = SqlStmt() { statements.add(e); }
    ( <SEMICOLON> e = SqlStmt() { statements.add(e); } )*
    [ <SEMICOLON> ]
    <END> <REQUEST>
    { return new SqlBeginRequestCall(s.end(this), statements); }
}

SqlNode ConditionalStmt() :
{
    final SqlNode e;
}
{
    (
        e = CaseStmt()
    |
        e = IfStmt()
    )
    { return e; }
}

SqlIfStmt IfStmt() :
{
    SqlNode e;
    final SqlNodeList conditionMultiStmtList = new SqlNodeList(getPos());
    final SqlStatementList elseMultiStmtList = new SqlStatementList(getPos());
}
{
    <IF> e = ConditionMultiStmtPair()
    { conditionMultiStmtList.add(e); }
    (
        <ELSE> <IF> e = ConditionMultiStmtPair()
        { conditionMultiStmtList.add(e); }
    )*
    [
        <ELSE>
        CreateProcedureStmtList(elseMultiStmtList)
    ]
    <END> <IF>
    {
        return new SqlIfStmt(getPos(), conditionMultiStmtList,
            elseMultiStmtList);
    }
}

SqlCaseStmt CaseStmt() :
{
    SqlNode firstOperand = null;
    SqlNode e = null;
    final SqlNodeList conditionMultiStmtList = new SqlNodeList(getPos());
    final SqlStatementList elseMultiStmtList = new SqlStatementList(getPos());
}
{
    <CASE>
    [ firstOperand = Expression(ExprContext.ACCEPT_NON_QUERY) ]
    (
        <WHEN> e = ConditionMultiStmtPair()
        { conditionMultiStmtList.add(e); }
    )+
    [
        <ELSE>
        CreateProcedureStmtList(elseMultiStmtList)
    ]
    <END> <CASE>
    {
        if (firstOperand == null) {
            return new SqlCaseStmtWithConditionalExpression(getPos(),
                conditionMultiStmtList, elseMultiStmtList);
        } else {
            return new SqlCaseStmtWithOperand(getPos(), firstOperand,
                conditionMultiStmtList, elseMultiStmtList);
        }
    }
}

SqlNode ConditionMultiStmtPair() :
{
    final SqlNode condition;
    final SqlStatementList multiStmtList = new SqlStatementList(getPos());
}
{
    condition = Expression(ExprContext.ACCEPT_NON_QUERY)
    <THEN>
    CreateProcedureStmtList(multiStmtList)
    {
        return new SqlConditionalStmtListPair(getPos(), condition,
            multiStmtList);
    }
}

SqlNode CursorStmt() :
{
    final SqlNode e;
}
{
    (
        e = SqlAllocateCursor()
    |
        e = SqlCloseCursor()
    |
        e = SqlDeallocatePrepare()
    |
        e = SqlDeleteUsingCursor()
    |
        e = SqlExecuteImmediate()
    |
        e = SqlExecuteStatement()
    |
        e = SqlFetchCursor()
    |
        e = SqlOpenCursor()
    |
        e = SqlPrepareStatement()
    |
        e = SqlSelectAndConsume()
    |
        e = SqlSelectInto()
    |
        e = SqlUpdateUsingCursor()
    )
    { return e; }
}

SqlAllocateCursor SqlAllocateCursor() :
{
    final SqlIdentifier cursorName;
    final SqlIdentifier procedureName;
    final Span s = Span.of();
}
{
    <ALLOCATE> cursorName = SimpleIdentifier()
    <CURSOR> <FOR> <PROCEDURE> procedureName = SimpleIdentifier()
    { return new SqlAllocateCursor(s.end(this), cursorName, procedureName); }
}

SqlDeleteUsingCursor SqlDeleteUsingCursor() :
{
    final SqlIdentifier tableName;
    final SqlIdentifier cursorName;
    final Span s = Span.of();
}
{
    ( <DELETE> | <DEL> ) <FROM> tableName = CompoundIdentifier()
    <WHERE> <CURRENT> <OF> cursorName = SimpleIdentifier()
    { return new SqlDeleteUsingCursor(s.end(this), tableName, cursorName); }
}

SqlExecuteImmediate SqlExecuteImmediate() :
{
    final SqlNode statementName;
    final Span s = Span.of();
}
{
    <EXECUTE> <IMMEDIATE>
    (
        statementName = SimpleIdentifier()
    |
        statementName = StringLiteral()
    )
    { return new SqlExecuteImmediate(s.end(this), statementName); }
}

SqlExecuteStatement SqlExecuteStatement() :
{
    final SqlIdentifier statementName;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    final Span s = Span.of();
    SqlNode e;
}
{
    <EXECUTE> statementName = SimpleIdentifier()
    [
        <USING>
        e = SimpleIdentifier() { parameters.add(e); }
        ( <COMMA> e = SimpleIdentifier() { parameters.add(e); } )*
    ]
    { return new SqlExecuteStatement(s.end(this), statementName, parameters); }
}

SqlDeallocatePrepare SqlDeallocatePrepare() :
{
    final SqlIdentifier statementName;
    final Span s = Span.of();
}
{
    <DEALLOCATE> <PREPARE> statementName = SimpleIdentifier()
    { return new SqlDeallocatePrepare(s.end(this), statementName); }
}

SqlCloseCursor SqlCloseCursor() :
{
    final SqlIdentifier cursorName;
    final Span s = Span.of();
}
{
    <CLOSE> cursorName = SimpleIdentifier()
    { return new SqlCloseCursor(s.end(this), cursorName); }
}

SqlDeclareCursor SqlDeclareCursor() :
{
    final SqlIdentifier cursorName;
    CursorScrollType scrollType = CursorScrollType.UNSPECIFIED;
    CursorReturnType returnType = CursorReturnType.UNSPECIFIED;
    CursorReturnToType returnToType = CursorReturnToType.UNSPECIFIED;
    CursorUpdateType updateType = CursorUpdateType.UNSPECIFIED;
    boolean only = false;
    SqlNode cursorSpecification = null;
    SqlIdentifier statementName = null;
    SqlIdentifier preparedStatementName = null;
    SqlNode prepareFrom = null;
    final Span s = Span.of();
}
{
    <DECLARE> cursorName = SimpleIdentifier()
    [
        (
            <SCROLL> { scrollType = CursorScrollType.SCROLL; }
        |
            <NO> <SCROLL> { scrollType = CursorScrollType.NO_SCROLL; }
        )
    ]
    <CURSOR>
    [
        (
            <WITHOUT> <RETURN> { returnType = CursorReturnType.WITHOUT_RETURN; }
        |
            <WITH> <RETURN> { returnType = CursorReturnType.WITH_RETURN; }
            [ <ONLY> { only = true; } ]
            [
                (
                    <TO> <CALLER> { returnToType = CursorReturnToType.CALLER; }
                |
                    <TO> <CLIENT> { returnToType = CursorReturnToType.CLIENT; }
                )
            ]
        )
    ]
    <FOR>
    (
        statementName = SimpleIdentifier()
    |
        LOOKAHEAD(OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY))
        cursorSpecification = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
        [
            <FOR>
            (
                <READ> <ONLY> { updateType = CursorUpdateType.READ_ONLY; }
            |
                <UPDATE> { updateType = CursorUpdateType.UPDATE; }
            )
        ]
    )
    [
        <PREPARE> preparedStatementName = SimpleIdentifier()
        <FROM>
        (
            prepareFrom = SimpleIdentifier()
        |
            prepareFrom = StringLiteral()
        )
    ]
    <SEMICOLON>
    {
        return new SqlDeclareCursor(s.end(this), cursorName, scrollType,
            returnType, returnToType, only, updateType, cursorSpecification,
            statementName, preparedStatementName, prepareFrom);
    }
}

SqlLiteral IntervalLiteral() :
{
    final String p;
    final int i;
    final SqlIntervalQualifier intervalQualifier;
    int sign = 1;
    final Span s;
}
{
    <INTERVAL> { s = span(); }
    [
        <MINUS> { sign = -1; }
    |
        <PLUS> { sign = 1; }
    ]
    (
        <QUOTED_STRING> { p = token.image; }
    |
        i = IntLiteral()
        // The single quotes are required as otherwise an exception gets
        // thrown during unparsing.
        { p = "'" + i + "'"; }
    )
    intervalQualifier = IntervalQualifier() {
        return SqlParserUtil.parseIntervalLiteral(s.end(intervalQualifier),
            sign, p, intervalQualifier);
    }
}

SqlPrepareStatement SqlPrepareStatement() :
{
    final SqlIdentifier statementName;
    final SqlNode statement;
    final Span s = Span.of();
}
{
    <PREPARE> statementName = SimpleIdentifier()
    <FROM>
    (
        statement = SimpleIdentifier()
    |
        statement = StringLiteral()
    )
    { return new SqlPrepareStatement(s.end(this), statementName, statement); }
}

SqlUpdateUsingCursor SqlUpdateUsingCursor() :
{
    final SqlIdentifier tableName;
    SqlIdentifier aliasName = null;
    final SqlNodeList assignments = new SqlNodeList(getPos());
    final SqlIdentifier cursorName;
    SqlNode e;
    final Span s = Span.of();
}
{
    ( <UPDATE> | <UPD> ) tableName = CompoundIdentifier()
    [ aliasName = SimpleIdentifier() ]
    <SET>
    e = Expression(ExprContext.ACCEPT_NON_QUERY) { assignments.add(e); }
    (
        <COMMA> e = Expression(ExprContext.ACCEPT_NON_QUERY) {
            assignments.add(e);
        }
    )*
    <WHERE> <CURRENT> <OF> cursorName = SimpleIdentifier()
    {
        return new SqlUpdateUsingCursor(s.end(this), tableName, aliasName,
            assignments, cursorName);
    }
}

SqlOpenCursor SqlOpenCursor() :
{
    final SqlIdentifier cursorName;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    final Span s = Span.of();
    SqlNode e;
}
{
    <OPEN> cursorName = SimpleIdentifier()
    [
        <USING>
        e = SimpleIdentifier() { parameters.add(e); }
        ( <COMMA> e = SimpleIdentifier() { parameters.add(e); } )*
    ]
    { return new SqlOpenCursor(s.end(this), cursorName, parameters); }
}

SqlFetchCursor SqlFetchCursor() :
{
    final FetchType fetchType;
    final SqlIdentifier cursorName;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    final Span s = Span.of();
    SqlNode e;
}
{
    <FETCH>
    (
        (
            <NEXT> { fetchType = FetchType.NEXT; }
        |
            <FIRST> { fetchType = FetchType.FIRST; }
        )
        <FROM>
    |
        [ <FROM> ] { fetchType = FetchType.UNSPECIFIED; }
    )
    cursorName = SimpleIdentifier()
    <INTO>
    e = SimpleIdentifier() { parameters.add(e); }
    ( <COMMA> e = SimpleIdentifier() { parameters.add(e); } )*
    {
        return new SqlFetchCursor(s.end(this), fetchType, cursorName,
            parameters);
    }
}

// This form of SELECT AND CONSUME is only valid inside a CREATE PROCEDURE
// statement.
SqlSelectAndConsume SqlSelectAndConsume() :
{
    final List<SqlNode> selectList;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    final SqlIdentifier fromTable;
    final int topNum;
    final Span s = Span.of();
    SqlNode e;
}
{
    ( <SELECT> | <SEL> )
    <AND> <CONSUME> <TOP> topNum = IntLiteral() {
        if (topNum != 1) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.numberLiteralOutOfRange(String.valueOf(topNum)));
        }
    }
    selectList = SelectList()
    <INTO>
    (
        e = SimpleIdentifier()
    |
        e = SqlHostVariable()
    )
    { parameters.add(e); }
    (
        <COMMA>
        (
            e = SimpleIdentifier()
        |
            e = SqlHostVariable()
        )
        { parameters.add(e); }
    )*
    <FROM>
    fromTable = CompoundIdentifier()
    {
        return new SqlSelectAndConsume(s.end(this),
            new SqlNodeList(selectList, Span.of(selectList).pos()), parameters,
            fromTable);
    }
}

SqlIterationStmt IterationStmt() :
{
    final SqlIterationStmt e;
}
{
    (
        LOOKAHEAD(3)
        e = ForStmt()
    |
        LOOKAHEAD(3)
        e = LoopStmt()
    |
        LOOKAHEAD(3)
        e = RepeatStmt()
    |
        e = WhileStmt()
    )
    { return e; }
}

SqlForStmt ForStmt() :
{
    final SqlIdentifier beginLabel;
    final SqlIdentifier endLabel;
    final SqlIdentifier forLoopVariable;
    final SqlIdentifier cursorName;
    final SqlNode cursorSpecification;
    final SqlStatementList statements = new SqlStatementList(getPos());
    final Span s = Span.of();
}
{
    (
        beginLabel = SimpleIdentifier() <COLON>
    |
        { beginLabel = null; }
    )
    <FOR> forLoopVariable = SimpleIdentifier()
    <AS>
    (
        cursorName = SimpleIdentifier()
        <CURSOR> <FOR>
    |
        { cursorName = null; }
    )
    cursorSpecification = SqlSelect()
    <DO>
    CreateProcedureStmtList(statements)
    <END> <FOR>
    (
        endLabel = SimpleIdentifier()
    |
        { endLabel = null; }
    )
    {
        return new SqlForStmt(s.end(this), statements, beginLabel, endLabel,
            forLoopVariable, cursorName, cursorSpecification);
    }
}

SqlWhileStmt WhileStmt() :
{
    final SqlIdentifier beginLabel;
    final SqlIdentifier endLabel;
    final SqlNode condition;
    final SqlStatementList statements = new SqlStatementList(getPos());
    final Span s = Span.of();
}
{
    (
        beginLabel = SimpleIdentifier() <COLON>
    |
        { beginLabel = null; }
    )
    <WHILE>
    condition = Expression(ExprContext.ACCEPT_NON_QUERY)
    <DO>
    CreateProcedureStmtList(statements)
    <END> <WHILE>
    (
        endLabel = SimpleIdentifier()
    |
        { endLabel = null; }
    )
    {
        return new SqlWhileStmt(s.end(this), condition, statements,
            beginLabel, endLabel);
    }
}

SqlRepeatStmt RepeatStmt() :
{
    final SqlIdentifier beginLabel;
    final SqlIdentifier endLabel;
    final SqlNode condition;
    final SqlStatementList statements = new SqlStatementList(getPos());
    final Span s = Span.of();
}
{
    (
        beginLabel = SimpleIdentifier() <COLON>
    |
        { beginLabel = null; }
    )
    <REPEAT>
    CreateProcedureStmtList(statements)
    <UNTIL>
    condition = Expression(ExprContext.ACCEPT_NON_QUERY)
    <END> <REPEAT>
    (
        endLabel = SimpleIdentifier()
    |
        { endLabel = null; }
    )
    {
        return new SqlRepeatStmt(s.end(this), condition, statements,
            beginLabel, endLabel);
    }
}

SqlLoopStmt LoopStmt() :
{
    final SqlIdentifier beginLabel;
    final SqlIdentifier endLabel;
    final SqlStatementList statements = new SqlStatementList(getPos());
    final Span s = Span.of();
}
{
    (
        beginLabel = SimpleIdentifier() <COLON>
    |
        { beginLabel = null; }
    )
    <LOOP>
    CreateProcedureStmtList(statements)
    <END> <LOOP>
    (
        endLabel = SimpleIdentifier()
    |
        { endLabel = null; }
    )
    {
        return new SqlLoopStmt(s.end(this), statements, beginLabel, endLabel);
    }
}

SqlNode DiagnosticStmt() :
{
    final SqlNode e;
}
{
    (
        e = SqlGetDiagnostics()
    |
        e = SqlSignal()
    )
    { return e; }
}

SqlSignal SqlSignal() :
{
    final SignalType signalType;
    SqlIdentifier conditionOrSqlState = null;
    SqlSetStmt setStmt = null;
    final Span s = Span.of();
}
{
    (
        <SIGNAL> { signalType = SignalType.SIGNAL; }
        conditionOrSqlState = SignalConditionOrSqlState()
    |
        <RESIGNAL> { signalType = SignalType.RESIGNAL; }
        [ conditionOrSqlState = SignalConditionOrSqlState() ]
    )
    [ setStmt = SetStmt() ]
    {
        return new SqlSignal(s.end(this), signalType, conditionOrSqlState,
            setStmt);
    }
}

SqlIdentifier SignalConditionOrSqlState() :
{
    final SqlIdentifier e;
}
{
    (
        e = SimpleIdentifier()
    |
        e = SqlState()
    )
    { return e; }
}

SqlGetDiagnostics SqlGetDiagnostics() :
{
    SqlNode conditionNumber = null;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    final Span s = Span.of();
    SqlNode e;
}
{
    <GET> <DIAGNOSTICS>
    [
        <EXCEPTION>
        (
            conditionNumber = SimpleIdentifier()
        |
            conditionNumber = NumericLiteral()
        )
    ]
    e = SqlGetDiagnosticsParam() { parameters.add(e); }
    ( <COMMA> e = SqlGetDiagnosticsParam() { parameters.add(e); } )*
    { return new SqlGetDiagnostics(s.end(this), conditionNumber, parameters); }
}

SqlGetDiagnosticsParam SqlGetDiagnosticsParam() :
{
    final SqlIdentifier name;
    final SqlIdentifier value;
    final Span s = Span.of();
}
{
    name = SimpleIdentifier()
    <EQ>
    value = SimpleIdentifier()
    { return new SqlGetDiagnosticsParam(s.end(this), name, value); }
}

SqlDeclareHandlerStmt SqlDeclareHandlerStmt() :
{
    final HandlerType handlerType;
    SqlIdentifier conditionName = null;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    SqlNode handlerStatement = null;
    final Span s = Span.of();
    SqlNode e;
}
{
    <DECLARE>
    (
        (
            <CONTINUE> { handlerType = HandlerType.CONTINUE; }
        |
            <EXIT> { handlerType = HandlerType.EXIT; }
        )
        <HANDLER>
    |
        conditionName = SimpleIdentifier()
        <CONDITION>
        { handlerType = HandlerType.CONDITION; }
    )
    [
        <FOR>
        (
            e = SqlState() { parameters.add(e); }
            ( <COMMA> e = SqlState() { parameters.add(e); })*
            [ handlerStatement = CreateProcedureStmt() ]
        |
            e = DeclareHandlerCondition() { parameters.add(e); }
            ( <COMMA> e = DeclareHandlerCondition() { parameters.add(e); })*
            handlerStatement = CreateProcedureStmt()
        )
    ]
    <SEMICOLON>
    {
        return new SqlDeclareHandlerStmt(s.end(this), handlerType, conditionName,
            parameters, handlerStatement);
    }
}

SqlState SqlState() :
{
    final Span s = Span.of();
    final String value;
}
{
    <SQLSTATE>
    [ <VALUE> ]
    <QUOTED_STRING> {
        value = SqlParserUtil.parseString(token.image);
        if (value.length() != 5) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.sqlStateCharLength(value));
        }
    }
    { return new SqlState(value, s.end(this)); }
}

SqlIdentifier DeclareHandlerCondition() :
{
    final Span s = Span.of();
    final DeclareHandlerConditionType conditionType;
    final SqlIdentifier condition;
}
{
    (
        (
            <SQLEXCEPTION> { conditionType = DeclareHandlerConditionType.SQLEXCEPTION; }
        |
            <SQLWARNING> { conditionType = DeclareHandlerConditionType.SQLWARNING; }
        |
            <NOT> <FOUND> { conditionType = DeclareHandlerConditionType.NOT_FOUND; }
        )
        { condition = new SqlDeclareHandlerCondition(s.end(this), conditionType); }
    |
        condition = SimpleIdentifier()
    )
    { return condition; }
}

SqlLeaveStmt LeaveStmt() :
{
    final SqlIdentifier label;
}
{
    <LEAVE> label = SimpleIdentifier()
    { return new SqlLeaveStmt(getPos(), label); }
}

SqlIterateStmt IterateStmt() :
{
    final SqlIdentifier label;
}
{
    <ITERATE> label = SimpleIdentifier()
    { return new SqlIterateStmt(getPos(), label); }
}

SqlSetStmt SetStmt() :
{
    final SqlIdentifier target;
    final SqlNode source;
    final Span s = Span.of();
}
{
    <SET>
    target = SimpleIdentifier()
    <EQ>
    source = Expression(ExprContext.ACCEPT_NON_QUERY)
    { return new SqlSetStmt(s.end(this), target, source); }
}

SqlSelectInto SqlSelectInto() :
{
    SqlSelectKeyword selectKeyword = SqlSelectKeyword.UNSPECIFIED;
    final List<SqlNode> selectList;
    final SqlNodeList parameters = new SqlNodeList(getPos());
    SqlNode fromClause = null;
    SqlNode whereClause = null;
    final Span s = Span.of();
    SqlNode e;
}
{
    ( <SELECT> | <SEL> )
    [
        (
            <ALL> { selectKeyword = SqlSelectKeyword.ALL; }
        |
            <DISTINCT> { selectKeyword = SqlSelectKeyword.DISTINCT; }
        )
    ]
    selectList = SelectList()
    <INTO>
    e = SimpleIdentifier() { parameters.add(e); }
    ( <COMMA> e = SimpleIdentifier() { parameters.add(e); } )*
    [ <FROM> fromClause = FromClause() ]
    whereClause = WhereOpt()
    {
        return new SqlSelectInto(s.end(this), selectKeyword,
            new SqlNodeList(selectList, Span.of(selectList).pos()), parameters,
            fromClause, whereClause);
    }
}

SqlCall FirstLastValue() :
{
    final SqlNode value;
    final SqlNode over;
    final List<SqlNode> args = new ArrayList<SqlNode>();
    final SqlKind kind;
    final SqlAggFunction function;
    final SqlCall firstLastCall;
}
{
    (
        <FIRST_VALUE> {
            function = SqlStdOperatorTable.FIRST_VALUE;
        }
    |
        <LAST_VALUE> {
            function = SqlStdOperatorTable.LAST_VALUE;
        }
    )
    <LPAREN>
    value = CompoundIdentifier() { args.add(value); }
    [
        (
            <IGNORE> {
                kind = SqlKind.IGNORE_NULLS;
            }
        |
            <RESPECT> {
                kind = SqlKind.RESPECT_NULLS;
            }
        )
        <NULLS> { args.add(new SqlNullTreatmentModifier(getPos(), kind)); }
    ]
    {
            firstLastCall = function.createCall(getPos(), args);
    }
    <RPAREN>
    <OVER>
    over = WindowSpecification()
    {
        return SqlStdOperatorTable.OVER.createCall(getPos(),
            firstLastCall, over);
    }
}

/**
 * Parses a reserved word which is used as the name of a function.
 */
SqlIdentifier ReservedFunctionName() :
{
}
{
    (
        <ABS>
    |   <AVG>
    |   <CARDINALITY>
    |   <CEILING>
    |   <CHAR_LENGTH>
    |   <CHARACTER_LENGTH>
    |   <COALESCE>
    |   <COLLECT>
    |   <COVAR_POP>
    |   <COVAR_SAMP>
    |   <CUME_DIST>
    |   <COUNT>
    |   <CURRENT_DATE>
    |   <CURRENT_TIME>
    |   <CURRENT_TIMESTAMP>
    |   <DENSE_RANK>
    |   <ELEMENT>
    |   <EVERY>
    |   <EXP>
    |   <FLOOR>
    |   <FUSION>
    |   <INTERSECTION>
    |   <GROUPING>
    |   <HOUR>
    |   <LAG>
    |   <LEAD>
    |   <LEFT>
    |   <LN>
    |   <LOCALTIME>
    |   <LOCALTIMESTAMP>
    |   <LOWER>
    |   <MAX>
    |   <MIN>
    |   <MINUTE>
    |   <MOD>
    |   <MONTH>
    |   <NTH_VALUE>
    |   <NTILE>
    |   <NULLIF>
    |   <OCTET_LENGTH>
    |   <PERCENT_RANK>
    |   <POWER>
    |   <RANK>
    |   <REGR_COUNT>
    |   <REGR_SXX>
    |   <REGR_SYY>
    |   <RIGHT>
    |   <ROW_NUMBER>
    |   <SECOND>
    |   <SOME>
    |   <SQRT>
    |   <STDDEV_POP>
    |   <STDDEV_SAMP>
    |   <SUM>
    |   <UPPER>
    |   <TRUNCATE>
    |   <USER>
    |   <VAR_POP>
    |   <VAR_SAMP>
    |   <YEAR>
    )
    {
        return new SqlIdentifier(unquotedIdentifier(), getPos());
    }
}
