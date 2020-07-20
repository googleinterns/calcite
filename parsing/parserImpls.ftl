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

/**
 * Parses an SQL statement.
 */
SqlNode SqlStmt() :
{
    SqlNode stmt;
}
{
    (
<#-- Add methods to parse additional statements here -->
<#list parser.statementParserMethods as method>
        LOOKAHEAD(2) stmt = ${method}
    |
</#list>
        stmt = SqlSetOption(Span.of(), null)
    |
        stmt = SqlAlter()
    |
<#if parser.createStatementParserMethods?size != 0>
        stmt = SqlCreate()
    |
</#if>
<#if parser.renameStatementParserMethods?size != 0>
        stmt = SqlRename()
    |
</#if>
<#if parser.execStatementParserMethods?size != 0>
        stmt = SqlExec()
    |
</#if>
<#if  parser.usingStatementParserMethods?size != 0>
        stmt = SqlUsing()
    |
</#if>
<#if parser.setTimeZoneStatementParserMethods?size != 0>
        stmt = SqlSetTimeZone()
    |
</#if>
<#if parser.allowUpsertFormOfUpdate>
        LOOKAHEAD(SqlUpdate() <ELSE>)
        stmt = SqlUpsert()
    |
</#if>
        stmt = SqlUpdate()
    |
        stmt = SqlInsert()
    |
<#if parser.dropStatementParserMethods?size != 0>
        stmt = SqlDrop()
    |
</#if>
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |
        stmt = SqlExplain()
    |
        stmt = SqlDescribe()
    |
        stmt = SqlDelete()
    |
        stmt = SqlMerge()
    |
        stmt = SqlProcedureCall()
    )
    {
        return stmt;
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

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
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

SqlCreate SqlCreateSchema() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final boolean ifNotExists;
    final SqlIdentifier id;
}
{
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
    <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    {
        return SqlDdlNodes.createSchema(s.end(this), createSpecifier, ifNotExists, id);
    }
}

SqlCreate SqlCreateForeignSchema() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNode type = null;
    SqlNode library = null;
    SqlNodeList optionList = null;
}
{
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
    <FOREIGN> <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
         <TYPE> type = StringLiteral()
    |
         <LIBRARY> library = StringLiteral()
    )
    [ optionList = Options() ]
    {
        return SqlDdlNodes.createForeignSchema(s.end(this), createSpecifier,
            ifNotExists, id, type, library, optionList);
    }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    final SqlNode constraint;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
            |
                <STORED> { strategy = ColumnStrategy.STORED; }
            |
                { strategy = ColumnStrategy.VIRTUAL; }
            )
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                strategy = ColumnStrategy.DEFAULT;
            }
        |
            {
                e = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), e, strategy));
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    )
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
    )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
            type.withNullable(nullable), e, null));
    }
}

SqlCreate SqlCreateType() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = AttributeDefList()
    |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), createSpecifier, id, attributeDefList, type);
    }
}

SqlCreate SqlCreateTable() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
}
{
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    [ tableElementList = TableElementList() ]
    [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
    {
        return SqlDdlNodes.createTable(s.end(this), createSpecifier, ifNotExists, id,
            tableElementList, query);
    }
}

SqlCreate SqlCreateView() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final SqlIdentifier id;
    final Pair<SqlNodeList, SqlNodeList> p;
    SqlNodeList columnList = null;
    final SqlNode query;
    boolean withCheckOption = false;
}
{
    (
        <CREATE> { s = span(); }
        (
            <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
        |
            { createSpecifier = SqlCreateSpecifier.CREATE; }
        )
    |
        <REPLACE>
        {
            s = span();
            createSpecifier = SqlCreateSpecifier.REPLACE;
        }
    )
    <VIEW> id = CompoundIdentifier()
    [ p = ParenthesizedCompoundIdentifierList() { columnList = p.left; } ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    [
        <WITH> <CHECK> <OPTION> { withCheckOption = true; }
    ]
    {
        return SqlDdlNodes.createView(s.end(this), createSpecifier, id, columnList,
            query, withCheckOption);
    }
}

SqlCreate SqlCreateMaterializedView() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
    <MATERIALIZED> <VIEW> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createMaterializedView(s.end(this), createSpecifier,
            ifNotExists, id, columnList, query);
    }
}

private void FunctionJarDef(SqlNodeList usingList) :
{
    final SqlDdlNodes.FileType fileType;
    final SqlNode uri;
}
{
    (
        <ARCHIVE> { fileType = SqlDdlNodes.FileType.ARCHIVE; }
    |
        <FILE> { fileType = SqlDdlNodes.FileType.FILE; }
    |
        <JAR> { fileType = SqlDdlNodes.FileType.JAR; }
    ) {
        usingList.add(SqlLiteral.createSymbol(fileType, getPos()));
    }
    uri = StringLiteral() {
        usingList.add(uri);
    }
}

SqlCreate SqlCreateFunction() :
{
    final Span s;
    final SqlCreateSpecifier createSpecifier;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <CREATE> { s = span(); }
    (
        <OR> <REPLACE> { createSpecifier = SqlCreateSpecifier.CREATE_OR_REPLACE; }
    |
        { createSpecifier = SqlCreateSpecifier.CREATE; }
    )
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    <AS>
    className = StringLiteral()
    [
        <USING> {
            usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
            <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return SqlDdlNodes.createFunction(s.end(this), createSpecifier, ifNotExists,
            id, className, usingList);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    final boolean foreign;
}
{
    (
        <FOREIGN> { foreign = true; }
    |
        { foreign = false; }
    )
    <SCHEMA> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropSchema(s.end(this), foreign, ifExists, id);
    }
}

SqlDrop SqlDropType(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TYPE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropType(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropMaterializedView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <MATERIALIZED> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropMaterializedView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <FUNCTION> ifExists = IfExistsOpt()
    id = CompoundIdentifier() {
        return SqlDdlNodes.dropFunction(s.end(this), ifExists, id);
    }
}

/**
 * Allows parser to be extended with new types of table references.  The
 * default implementation of this production is empty.
 */
SqlNode ExtendedTableRef() :
{
}
{
    UnusedExtension()
    {
        return null;
    }
}

/**
 * Allows an OVER clause following a table expression as an extension to
 * standard SQL syntax. The default implementation of this production is empty.
 */
SqlNode TableOverOpt() :
{
}
{
    {
        return null;
    }
}

/*
 * Parses dialect-specific keywords immediately following the SELECT keyword.
 */
void SqlSelectKeywords(List<SqlLiteral> keywords) :
{}
{
    E()
}

/*
 * Parses dialect-specific keywords immediately following the INSERT keyword.
 */
void SqlInsertKeywords(List<SqlLiteral> keywords) :
{}
{
    E()
}

/*
* Parse Floor/Ceil function parameters
*/
SqlNode FloorCeilOptions(Span s, boolean floorFlag) :
{
    SqlNode node;
}
{
    node = StandardFloorCeilOptions(s, floorFlag) {
        return node;
    }
}

/*****************************************
 * Syntactical Descriptions              *
 *****************************************/

/**
 * Parses either a row expression or a query expression with an optional
 * ORDER BY.
 *
 * <p>Postgres syntax for limit:
 *
 * <blockquote><pre>
 *    [ LIMIT { count | ALL } ]
 *    [ OFFSET start ]</pre>
 * </blockquote>
 *
 * <p>MySQL syntax for limit:
 *
 * <blockquote><pre>
 *    [ LIMIT { count | start, count } ]</pre>
 * </blockquote>
 *
 * <p>SQL:2008 syntax for limit:
 *
 * <blockquote><pre>
 *    [ OFFSET start { ROW | ROWS } ]
 *    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]</pre>
 * </blockquote>
 */
SqlNode OrderedQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
    SqlNodeList orderBy = null;
    SqlNode start = null;
    SqlNode count = null;
}
{
    (
        e = QueryOrExpr(exprContext)
    )
    [
        // use the syntactic type of the expression we just parsed
        // to decide whether ORDER BY makes sense
        orderBy = OrderBy(e.isA(SqlKind.QUERY))
    ]
    [
        // Postgres-style syntax. "LIMIT ... OFFSET ..."
        <LIMIT>
        (
            // MySQL-style syntax. "LIMIT start, count"
            LOOKAHEAD(2)
            start = UnsignedNumericLiteralOrParam()
            <COMMA> count = UnsignedNumericLiteralOrParam() {
                if (!this.conformance.isLimitStartCountAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.limitStartCountNotAllowed());
                }
            }
        |
            count = UnsignedNumericLiteralOrParam()
        |
            <ALL>
        )
    ]
    [
        // ROW or ROWS is required in SQL:2008 but we make it optional
        // because it is not present in Postgres-style syntax.
        // If you specify both LIMIT start and OFFSET, OFFSET wins.
        <OFFSET> start = UnsignedNumericLiteralOrParam() [ <ROW> | <ROWS> ]
    ]
    [
        // SQL:2008-style syntax. "OFFSET ... FETCH ...".
        // If you specify both LIMIT and FETCH, FETCH wins.
        <FETCH> ( <FIRST> | <NEXT> ) count = UnsignedNumericLiteralOrParam()
        ( <ROW> | <ROWS> ) <ONLY>
    ]
    {
        if (orderBy != null || start != null || count != null) {
            if (orderBy == null) {
                orderBy = SqlNodeList.EMPTY;
            }
            e = new SqlOrderBy(getPos(), e, orderBy, start, count);

        }
        return e;
    }
}

/**
 * Parses a leaf in a query expression (SELECT, VALUES or TABLE).
 */
SqlNode LeafQuery(ExprContext exprContext) :
{
    SqlNode e;
}
{
    {
        // ensure a query is legal in this context
        checkQueryExpression(exprContext);
    }
    e = SqlSelect() { return e; }
|
    e = TableConstructor() { return e; }
|
    e = ExplicitTable(getPos()) { return e; }
}

/**
 * Parses a parenthesized query or single row expression.
 */
SqlNode ParenthesizedExpression(ExprContext exprContext) :
{
    SqlNode e;
}
{
    <LPAREN>
    {
        // we've now seen left paren, so queries inside should
        // be allowed as sub-queries
        switch (exprContext) {
        case ACCEPT_SUB_QUERY:
            exprContext = ExprContext.ACCEPT_NONCURSOR;
            break;
        case ACCEPT_CURSOR:
            exprContext = ExprContext.ACCEPT_ALL;
            break;
        }
    }
    e = OrderedQueryOrExpr(exprContext)
    <RPAREN>
    {
        return e;
    }
}

/**
 * Parses a parenthesized query or comma-list of row expressions.
 *
 * <p>REVIEW jvs 8-Feb-2004: There's a small hole in this production.  It can be
 * used to construct something like
 *
 * <blockquote><pre>
 * WHERE x IN (select count(*) from t where c=d,5)</pre>
 * </blockquote>
 *
 * <p>which should be illegal.  The above is interpreted as equivalent to
 *
 * <blockquote><pre>
 * WHERE x IN ((select count(*) from t where c=d),5)</pre>
 * </blockquote>
 *
 * <p>which is a legal use of a sub-query.  The only way to fix the hole is to
 * be able to remember whether a subexpression was parenthesized or not, which
 * means preserving parentheses in the SqlNode tree.  This is probably
 * desirable anyway for use in purely syntactic parsing applications (e.g. SQL
 * pretty-printer).  However, if this is done, it's important to also make
 * isA() on the paren node call down to its operand so that we can
 * always correctly discriminate a query from a row expression.
 */
SqlNodeList ParenthesizedQueryOrCommaList(
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
    e = OrderedQueryOrExpr(firstExprContext)
    {
        list = startList(e);
    }
    (
        <COMMA>
        {
            // a comma-list can't appear where only a query is expected
            checkNonQueryExpression(exprContext);
        }
        e = Expression(exprContext)
        {
            list.add(e);
        }
    )*
    <RPAREN>
    {
        return new SqlNodeList(list, s.end(this));
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
<#if parser.includeInsertWithOmittedValues>
    |
        // This LOOKAHEAD ensures that parsing fails if there is an empty set of
        // parentheses after a VALUES keyword since that is invalid syntax in
        // most dialects
        LOOKAHEAD({ getToken(1).kind != RPAREN })
        { e = SqlLiteral.createNull(getPos()); }
</#if>
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
<#if parser.includeInsertWithOmittedValues>
        |
            { e = SqlLiteral.createNull(getPos()); }
</#if>
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
 * Parses function parameter lists.
 * If the list starts with DISTINCT or ALL, it is discarded.
 */
List UnquantifiedFunctionParameterList(
    ExprContext exprContext) :
{
    final List args;
}
{
    args = FunctionParameterList(exprContext) {
        final SqlLiteral quantifier = (SqlLiteral) args.get(0);
        args.remove(0); // remove DISTINCT or ALL, if present
        return args;
    }
}

/**
 * Parses function parameter lists including DISTINCT keyword recognition,
 * DEFAULT, and named argument assignment.
 */
List FunctionParameterList(
    ExprContext exprContext) :
{
    SqlNode e = null;
    List list = new ArrayList();
}
{
    <LPAREN>
    [
        <DISTINCT> {
            e = SqlSelectKeyword.DISTINCT.symbol(getPos());
        }
    |
        <ALL> {
            e = SqlSelectKeyword.ALL.symbol(getPos());
        }
    ]
    {
        list.add(e);
    }
    Arg0(list, exprContext)
    (
        <COMMA> {
            // a comma-list can't appear where only a query is expected
            checkNonQueryExpression(exprContext);
        }
        Arg(list, exprContext)
    )*
    <RPAREN>
    {
        return list;
    }
}

void Arg0(List list, ExprContext exprContext) :
{
    SqlIdentifier name = null;
    SqlNode e = null;
    final ExprContext firstExprContext;
    {
        // we've now seen left paren, so queries inside should
        // be allowed as sub-queries
        switch (exprContext) {
        case ACCEPT_SUB_QUERY:
            firstExprContext = ExprContext.ACCEPT_NONCURSOR;
            break;
        case ACCEPT_CURSOR:
            firstExprContext = ExprContext.ACCEPT_ALL;
            break;
        default:
            firstExprContext = exprContext;
            break;
        }
    }
}
{
    [
        LOOKAHEAD(2) name = SimpleIdentifier() <NAMED_ARGUMENT_ASSIGNMENT>
    ]
    (
        e = Default()
    |
        e = OrderedQueryOrExpr(firstExprContext)
    )
    {
        if (e != null) {
            if (name != null) {
                e = SqlStdOperatorTable.ARGUMENT_ASSIGNMENT.createCall(
                    Span.of(name, e).pos(), e, name);
            }
            list.add(e);
        }
    }
}

void Arg(List list, ExprContext exprContext) :
{
    SqlIdentifier name = null;
    SqlNode e = null;
}
{
    [
        LOOKAHEAD(2) name = SimpleIdentifier() <NAMED_ARGUMENT_ASSIGNMENT>
    ]
    (
        e = Default()
    |
        e = Expression(exprContext)
    )
    {
        if (e != null) {
            if (name != null) {
                e = SqlStdOperatorTable.ARGUMENT_ASSIGNMENT.createCall(
                    Span.of(name, e).pos(), e, name);
            }
            list.add(e);
        }
    }
}

SqlNode Default() : {}
{
    <DEFAULT_> {
        return SqlStdOperatorTable.DEFAULT.createCall(getPos());
    }
}

/**
 * Parses a query (SELECT, UNION, INTERSECT, EXCEPT, VALUES, TABLE) followed by
 * the end-of-file symbol.
 */
SqlNode SqlQueryEof() :
{
    SqlNode query;
}
{
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    <EOF>
    { return query; }
}

/**
 * Parses a list of SQL statements separated by semicolon with an <EOF> expected.
 * The semicolon is required between statements, but is
 * optional at the end.
 */
SqlNodeList SqlStmtListEof() :
{
    final SqlNodeList stmtList;
}
{
    stmtList = SqlStmtList()
    <EOF>
    {
        return stmtList;
    }
}

/**
 * Parses a list of SQL statements separated by semicolon.
 * The semicolon is required between statements, but is
 * optional at the end.
 */
SqlNodeList SqlStmtList() :
{
    final List<SqlNode> stmtList = new ArrayList<SqlNode>();
    SqlNode stmt;
}
{
    stmt = SqlStmt() {
        stmtList.add(stmt);
    }
    (
        <SEMICOLON>
        [
            stmt = SqlStmt() {
                stmtList.add(stmt);
            }
        ]
    )*
    {
        return new SqlNodeList(stmtList, Span.of(stmtList).pos());
    }
}

/**
 * Parses an SQL statement followed by the end-of-file symbol.
 */
SqlNode SqlStmtEof() :
{
    SqlNode stmt;
}
{
    stmt = SqlStmt() <EOF>
    {
        return stmt;
    }
}

SqlNodeList ParenthesizedKeyValueOptionCommaList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    { s = span(); }
    <LPAREN>
    KeyValueOption(list)
    (
        <COMMA>
        KeyValueOption(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

/**
* Parses an option with format key=val whose key is a simple identifier or string literal
* and value is a string literal.
*/
void KeyValueOption(List<SqlNode> list) :
{
    final SqlNode key;
    final SqlNode value;
}
{
    (
        key = SimpleIdentifier()
    |
        key = StringLiteral()
    )
    <EQ>
    value = StringLiteral() {
        list.add(key);
        list.add(value);
    }
}

/**
* Parses an option value, it's either a string or a numeric.
*/
SqlNode OptionValue() :
{
    final SqlNode value;
}
{
    (
        value = NumericLiteral()
    |
        value = StringLiteral()
    )
    {
        return value;
    }
}

/**
 * Parses a literal list separated by comma. The literal is either a string or a numeric.
 */
SqlNodeList ParenthesizedLiteralOptionCommaList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode optionVal;
}
{
    { s = span(); }
    <LPAREN>
    optionVal = OptionValue()
    {
        list.add(optionVal);
    }
    (
        <COMMA>
        optionVal = OptionValue()
        {
            list.add(optionVal);
        }
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void CommaSepatatedSqlHints(List<SqlNode> hints) :
{
    SqlIdentifier hintName;
    SqlNodeList hintOptions;
    SqlNode optionVal;
    SqlHint.HintOptionFormat optionFormat;
}
{
    hintName = SimpleIdentifier()
    (   LOOKAHEAD(5)
        hintOptions = ParenthesizedKeyValueOptionCommaList() {
            optionFormat = SqlHint.HintOptionFormat.KV_LIST;
        }
    |
        LOOKAHEAD(3)
        hintOptions = ParenthesizedSimpleIdentifierList() {
            optionFormat = SqlHint.HintOptionFormat.ID_LIST;
        }
    |
        LOOKAHEAD(3)
        hintOptions = ParenthesizedLiteralOptionCommaList() {
            optionFormat = SqlHint.HintOptionFormat.LITERAL_LIST;
        }
    |
        LOOKAHEAD(2)
        [<LPAREN> <RPAREN>]
        {
            hintOptions = SqlNodeList.EMPTY;
            optionFormat = SqlHint.HintOptionFormat.EMPTY;
        }
    )
    {
        hints.add(new SqlHint(Span.of(hintOptions).end(this), hintName, hintOptions, optionFormat));
    }
    (
        <COMMA>
        hintName = SimpleIdentifier()
        (
            LOOKAHEAD(5)
            hintOptions = ParenthesizedKeyValueOptionCommaList() {
                optionFormat = SqlHint.HintOptionFormat.KV_LIST;
            }
        |
            LOOKAHEAD(3)
            hintOptions = ParenthesizedSimpleIdentifierList() {
                optionFormat = SqlHint.HintOptionFormat.ID_LIST;
            }
        |
            LOOKAHEAD(3)
            hintOptions = ParenthesizedLiteralOptionCommaList() {
                optionFormat = SqlHint.HintOptionFormat.LITERAL_LIST;
            }
        |
            LOOKAHEAD(2)
            [<LPAREN> <RPAREN>]
            {
                hintOptions = SqlNodeList.EMPTY;
                optionFormat = SqlHint.HintOptionFormat.EMPTY;
            }
        )
        {
            hints.add(new SqlHint(Span.of(hintOptions).end(this), hintName, hintOptions, optionFormat));
        }
    )*
}

/**
 * Parses a table reference with optional hints.
 */
SqlNode TableRefWithHintsOpt() :
{
    SqlNode tableRef;
    SqlNodeList hintList;
    final List<SqlNode> hints = new ArrayList<SqlNode>();
    final Span s;
}
{
    { s = span(); }
    tableRef = CompoundIdentifier()
    [
        LOOKAHEAD(2)
        <HINT_BEG>
        CommaSepatatedSqlHints(hints)
        <COMMENT_END>
        {
            hintList = new SqlNodeList(hints, s.addAll(hints).end(this));
            tableRef = new SqlTableRef(Span.of(tableRef, hintList).pos(),
                    (SqlIdentifier) tableRef, hintList);
        }
    ]
    {
        return tableRef;
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
    SqlNode exceptExpression = null;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlNodeList groupBy;
    final SqlNode having;
    SqlNode qualify = null;
    final SqlNodeList windowDecls;
    final List<SqlNode> hints = new ArrayList<SqlNode>();
    final Span s;
}
{
    (
        <SELECT>
<#if parser.allowAbbreviatedKeywords>
    |
        <SEL>
</#if>
    )
    {
        s = span();
    }
    [
        <HINT_BEG>
        CommaSepatatedSqlHints(hints)
        <COMMENT_END>
    ]
    SqlSelectKeywords(keywords)
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
<#if parser.includeTopN>
    |
        topN = SqlSelectTopN(getPos())
</#if>
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectList()
<#if parser.includeExceptExpression>
    [
        exceptExpression = ExceptExpression(selectList)
    ]
</#if>
    (
        <FROM> fromClause = FromClause()
        where = WhereOpt()
        groupBy = GroupByOpt()
        having = HavingOpt()
<#if parser.includeQualifyClause>
        qualify = QualifyOpt()
</#if>
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            where = null;
            groupBy = null;
            having = null;
            qualify = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelect(s.end(this), keywordList, topN,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            exceptExpression, fromClause, where, groupBy, having, qualify,
            windowDecls, /*orderBy=*/ null, /*offset=*/ null, /*fetch=*/ null,
            new SqlNodeList(hints, getPos()));
    }
}

/*
 * Abstract production:
 *
 *    void SqlSelectKeywords(List keywords)
 *
 * Parses dialect-specific keywords immediately following the SELECT keyword.
 */

/**
 * Parses an EXPLAIN PLAN statement.
 */
SqlNode SqlExplain() :
{
    SqlNode stmt;
    SqlExplainLevel detailLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    SqlExplain.Depth depth;
    final SqlExplainFormat format;
}
{
    <EXPLAIN> <PLAN>
    [ detailLevel = ExplainDetailLevel() ]
    depth = ExplainDepth()
    (
        LOOKAHEAD(2)
        <AS> <XML> { format = SqlExplainFormat.XML; }
    |
        <AS> <JSON> { format = SqlExplainFormat.JSON; }
    |
        { format = SqlExplainFormat.TEXT; }
    )
    <FOR> stmt = SqlQueryOrDml() {
        return new SqlExplain(getPos(),
            stmt,
            detailLevel.symbol(SqlParserPos.ZERO),
            depth.symbol(SqlParserPos.ZERO),
            format.symbol(SqlParserPos.ZERO),
            nDynamicParams);
    }
}

/** Parses a query (SELECT or VALUES)
 * or DML statement (INSERT, UPDATE, DELETE, MERGE). */
SqlNode SqlQueryOrDml() :
{
    SqlNode stmt;
}
{
    (
        stmt = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |
        stmt = SqlInsert()
    |
        stmt = SqlDelete()
    |
        stmt = SqlUpdate()
    |
        stmt = SqlMerge()
    ) { return stmt; }
}

/**
 * Parses WITH TYPE | WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION modifier for
 * EXPLAIN PLAN.
 */
SqlExplain.Depth ExplainDepth() :
{
}
{
    (
        LOOKAHEAD(2)
        <WITH> <TYPE>
        {
            return SqlExplain.Depth.TYPE;
        }
        |
        <WITH> <IMPLEMENTATION>
        {
            return SqlExplain.Depth.PHYSICAL;
        }
        |
        <WITHOUT> <IMPLEMENTATION>
        {
            return SqlExplain.Depth.LOGICAL;
        }
        |
        {
            return SqlExplain.Depth.PHYSICAL;
        }

    )
}

/**
 * Parses INCLUDING ALL ATTRIBUTES modifier for EXPLAIN PLAN.
 */
SqlExplainLevel ExplainDetailLevel() :
{
    SqlExplainLevel level = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
}
{
    (
        <EXCLUDING> <ATTRIBUTES>
        {
            level = SqlExplainLevel.NO_ATTRIBUTES;
        }
        |
        <INCLUDING>
        [ <ALL> { level = SqlExplainLevel.ALL_ATTRIBUTES; } ]
        <ATTRIBUTES>
        {
        }
    )
    {
        return level;
    }
}

/**
 * Parses a DESCRIBE statement.
 */
SqlNode SqlDescribe() :
{
   final Span s;
   final SqlIdentifier table;
   final SqlIdentifier column;
   final SqlIdentifier id;
   final SqlNode stmt;
}
{
    <DESCRIBE> { s = span(); }
    (
        LOOKAHEAD(2) (<DATABASE> | <CATALOG> | <SCHEMA>)
        id = CompoundIdentifier() {
            // DESCRIBE DATABASE and DESCRIBE CATALOG currently do the same as
            // DESCRIBE SCHEMA but should be different. See
            //   [CALCITE-1221] Implement DESCRIBE DATABASE, CATALOG, STATEMENT
            return new SqlDescribeSchema(s.end(id), id);
        }
    |
        // Use syntactic lookahead to determine whether a table name is coming.
        // We do not allow SimpleIdentifier() because that includes <STATEMENT>.
        LOOKAHEAD( <TABLE> | <IDENTIFIER> | <QUOTED_IDENTIFIER>
           | <BACK_QUOTED_IDENTIFIER> | <BRACKET_QUOTED_IDENTIFIER> )
        (<TABLE>)?
        table = CompoundIdentifier()
        (
            column = SimpleIdentifier()
        |
            E() { column = null; }
        ) {
            return new SqlDescribeTable(s.add(table).addIf(column).pos(),
                table, column);
        }
    |
        (LOOKAHEAD(1) <STATEMENT>)?
        stmt = SqlQueryOrDml() {
            // DESCRIBE STATEMENT currently does the same as EXPLAIN. See
            //   [CALCITE-1221] Implement DESCRIBE DATABASE, CATALOG, STATEMENT
            final SqlExplainLevel detailLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
            final SqlExplain.Depth depth = SqlExplain.Depth.PHYSICAL;
            final SqlExplainFormat format = SqlExplainFormat.TEXT;
            return new SqlExplain(s.end(stmt),
                stmt,
                detailLevel.symbol(SqlParserPos.ZERO),
                depth.symbol(SqlParserPos.ZERO),
                format.symbol(SqlParserPos.ZERO),
                nDynamicParams);
        }
    )
}

/**
 * Parses a CALL statement.
 */
SqlNode SqlProcedureCall() :
{
    final Span s;
    SqlNode routineCall;
}
{
    <CALL> {
        s = span();
    }
    routineCall = NamedRoutineCall(
        SqlFunctionCategory.USER_DEFINED_PROCEDURE,
        ExprContext.ACCEPT_SUB_QUERY)
    {
        return SqlStdOperatorTable.PROCEDURE_CALL.createCall(
            s.end(routineCall), routineCall);
    }
}

SqlNode NamedRoutineCall(
    SqlFunctionCategory routineType,
    ExprContext exprContext) :
{
    SqlIdentifier name;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    name = CompoundIdentifier() {
        s = span();
    }
    <LPAREN>
    [
        Arg0(list, exprContext)
        (
            <COMMA> {
                // a comma-list can't appear where only a query is expected
                checkNonQueryExpression(exprContext);
            }
            Arg(list, exprContext)
        )*
    ]
    <RPAREN>
    {
        return createCall(name, s.end(this), routineType, null, list);
    }
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
<#if parser.allowAbbreviatedKeywords>
    |
        <INS>
</#if>
    |
        <UPSERT> { keywords.add(SqlInsertKeyword.UPSERT.symbol(getPos())); }
    )
    { s = span(); }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
<#if parser.allowUpsertFormOfUpdate>
    [ <INTO> ]
<#else>
    <INTO>
</#if>
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
<#-- additional literal parser methods are included here -->
<#list parser.insertStatementParserMethods as method>
        LOOKAHEAD(${method}())
        source = ${method}()
    |
</#list>
        source = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    )
    {
        return new SqlInsert(s.end(source), keywordList, table, source,
            columnList);
    }
}

/*
 * Abstract production:
 *
 *    void SqlInsertKeywords(List keywords)
 *
 * Parses dialect-specific keywords immediately following the INSERT keyword.
 */

/**
 * Parses a DELETE statement.
 */
SqlNode SqlDelete() :
{
    SqlNode table;
    SqlNodeList extendList = null;
    SqlIdentifier alias = null;
    final SqlNode condition;
    final Span s;
}
{
    (
        <DELETE>
<#if parser.allowAbbreviatedKeywords>
    |
        <DEL>
</#if>
    )
    { s = span(); }
<#if parser.allowOptionalFromInDelete>
    [ <FROM> ]
<#else>
    <FROM>
</#if>
    table = TableRefWithHintsOpt()
    [
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [ [ <AS> ] alias = SimpleIdentifier() ]
    condition = WhereOpt()
    {
        return new SqlDelete(s.add(table).addIf(extendList).addIf(alias)
            .addIf(condition).pos(), table, condition, null, alias);
    }
}

/**
 * Parses an UPDATE statement.
 */
SqlNode SqlUpdate() :
{
    SqlNode table;
    SqlNodeList sourceTables = null;
    SqlNodeList sourceAliases = null;
    SqlNodeList extendList = null;
    SqlIdentifier alias = null;
    SqlNode condition;
    SqlNodeList sourceExpressionList;
    SqlNodeList targetColumnList;
    SqlIdentifier id;
    SqlNode exp;
    final Span s;
}
{
    (
        <UPDATE>
<#if parser.allowAbbreviatedKeywords>
    |
        <UPD>
</#if>
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
<#if parser.allowUpdateFromTable>
    [
        (
            <FROM> {
                sourceTables = new SqlNodeList(s.pos());
                sourceAliases = new SqlNodeList(s.pos());
            }
            SourceTableAndAlias(sourceTables, sourceAliases)
        )
        (
            <COMMA>
            SourceTableAndAlias(sourceTables, sourceAliases)
        )*
    ]
</#if>
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
            sourceTables, sourceAliases);
    }
}

/**
 * Parses a MERGE statement.
 */
SqlNode SqlMerge() :
{
    SqlNode table;
    SqlNodeList extendList = null;
    SqlIdentifier alias = null;
    SqlNode sourceTableRef;
    SqlNode condition;
    SqlUpdate updateCall = null;
    SqlInsert insertCall = null;
    final Span s;
}
{
    <MERGE> { s = span(); } <INTO> table = TableRefWithHintsOpt()
    [
        [ <EXTEND> ]
        extendList = ExtendList() {
            table = extend(table, extendList);
        }
    ]
    [ [ <AS> ] alias = SimpleIdentifier() ]
    <USING> sourceTableRef = TableRef()
    <ON> condition = Expression(ExprContext.ACCEPT_SUB_QUERY)
    (
        LOOKAHEAD(2)
        updateCall = WhenMatchedClause(table, alias)
        [ insertCall = WhenNotMatchedClause(table) ]
    |
        insertCall = WhenNotMatchedClause(table)
    )
    {
        return new SqlMerge(s.addIf(updateCall).addIf(insertCall).pos(), table,
            condition, sourceTableRef, updateCall, insertCall, null, alias);
    }
}

SqlUpdate WhenMatchedClause(SqlNode table, SqlIdentifier alias) :
{
    SqlIdentifier id;
    final Span s;
    final SqlNodeList updateColumnList = new SqlNodeList(SqlParserPos.ZERO);
    SqlNode exp;
    final SqlNodeList updateExprList = new SqlNodeList(SqlParserPos.ZERO);
}
{
    <WHEN> { s = span(); } <MATCHED> <THEN>
    <UPDATE> <SET> id = CompoundIdentifier() {
        updateColumnList.add(id);
    }
    <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        updateExprList.add(exp);
    }
    (
        <COMMA>
        id = CompoundIdentifier() {
            updateColumnList.add(id);
        }
        <EQ> exp = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            updateExprList.add(exp);
        }
    )*
    {
        return new SqlUpdate(s.addAll(updateExprList).pos(), table,
            updateColumnList, updateExprList, null, null, alias,
            /*secondTable=*/ null, /*secondAlias=*/ null);
    }
}

SqlInsert WhenNotMatchedClause(SqlNode table) :
{
    final Span insertSpan, valuesSpan;
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    final Pair<SqlNodeList, SqlNodeList> nameAndTypePair;
    SqlNodeList insertColumnList = null;
    final SqlNode rowConstructor;
    final SqlNode insertValues;
}
{
    <WHEN> <NOT> <MATCHED> <THEN> <INSERT> {
        insertSpan = span();
    }
    SqlInsertKeywords(keywords) {
        keywordList = new SqlNodeList(keywords, insertSpan.end(this));
    }
    [
        LOOKAHEAD(2)
        nameAndTypePair = ParenthesizedCompoundIdentifierList()
        { insertColumnList = nameAndTypePair.getKey(); }
    ]
    [ <LPAREN> ]
    <VALUES> { valuesSpan = span(); }
    rowConstructor = RowConstructor()
    [ <RPAREN> ]
    {
        // TODO zfong 5/26/06: note that extra parentheses are accepted above
        // around the VALUES clause as a hack for unparse, but this is
        // actually invalid SQL; should fix unparse
        insertValues = SqlStdOperatorTable.VALUES.createCall(
            valuesSpan.end(this), rowConstructor);
        return new SqlInsert(insertSpan.end(this), keywordList,
            table, insertValues, insertColumnList);
    }
}

/**
 * Parses the select list of a SELECT statement.
 */
List<SqlNode> SelectList() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode item;
}
{
    item = SelectItem() {
        list.add(item);
    }
    (
        <COMMA> item = SelectItem() {
            list.add(item);
        }
    )*
    {
        return list;
    }
}

/**
 * Parses one item in a select list.
 */
SqlNode SelectItem() :
{
    SqlNode e;
    final SqlIdentifier id;
}
{
    e = SelectExpression()
    [
        [ <AS> ]
        id = SimpleIdentifier() {
            e = SqlStdOperatorTable.AS.createCall(span().end(e), e, id);
        }
    ]
    {
        return e;
    }
}

/**
 * Parses one unaliased expression in a select list.
 */
SqlNode SelectExpression() :
{
    SqlNode e;
}
{
    <STAR> {
        return SqlIdentifier.star(getPos());
    }
|
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        return e;
    }
}

SqlLiteral Natural() :
{
}
{
    <NATURAL> { return SqlLiteral.createBoolean(true, getPos()); }
|
    { return SqlLiteral.createBoolean(false, getPos()); }
}

SqlLiteral JoinType() :
{
    JoinType joinType;
}
{
    (
    LOOKAHEAD(3) // required for "LEFT SEMI JOIN" in Hive
<#list parser.joinTypes as method>
        joinType = ${method}()
    |
</#list>
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

/**
 * Parses either a table reference or a nested JOIN clause.
 */
SqlNode TableRefOrJoinClause() :
{
    SqlNode e;
}
{
    (
<#if parser.allowNestedJoins>
        LOOKAHEAD(<LPAREN> TableRef() Natural() JoinType())
        <LPAREN>
        e = TableRef()
        e = JoinClause(e)
        <RPAREN>
    |
</#if>
        e = TableRef()
    )
    { return e; }
}

/** Matches "LEFT JOIN t ON ...", "RIGHT JOIN t USING ...", "JOIN t". */
SqlNode JoinClause(SqlNode e) :
{
    SqlNode e2, condition;
    final SqlLiteral natural, joinType, joinConditionType;
    SqlNodeList list;
}
{
    natural = Natural()
    joinType = JoinType()
    e2 = TableRefOrJoinClause()
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

// TODO jvs 15-Nov-2003:  SQL standard allows parentheses in the FROM list for
// building up non-linear join trees (e.g. OUTER JOIN two tables, and then INNER
// JOIN the result).  Also note that aliases on parenthesized FROM expressions
// "hide" all table names inside the parentheses (without aliases, they're
// visible).
//
// We allow CROSS JOIN to have a join condition, even though that is not valid
// SQL; the validator will catch it.
/**
 * Parses the FROM clause for a SELECT.
 *
 * <p>FROM is mandatory in standard SQL, optional in dialects such as MySQL,
 * PostgreSQL. The parser allows SELECT without FROM, but the validator fails
 * if conformance is, say, STRICT_2003.
 */
SqlNode FromClause() :
{
    SqlNode e, e2, condition;
    SqlLiteral natural, joinType, joinConditionType;
    SqlNodeList list;
    SqlParserPos pos;
}
{
    e = TableRef()
    (
        LOOKAHEAD(2)
        (
            // Decide whether to read a JOIN clause or a comma, or to quit having
            // seen a single entry FROM clause like 'FROM emps'. See comments
            // elsewhere regarding <COMMA> lookahead.
            //
            // And LOOKAHEAD(3) is needed here rather than a LOOKAHEAD(2). Because currently JavaCC
            // calculates minimum lookahead count incorrectly for choice that contains zero size
            // child. For instance, with the generated code, "LOOKAHEAD(2, Natural(), JoinType())"
            // returns true immediately if it sees a single "<CROSS>" token. Where we expect
            // the lookahead succeeds after "<CROSS> <APPLY>".
            //
            // For more information about the issue, see https://github.com/javacc/javacc/issues/86
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
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <CROSS> { joinType = JoinType.CROSS.symbol(getPos()); } <APPLY>
            e2 = TableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
                    null);
            }
        |
            <OUTER> { joinType = JoinType.LEFT.symbol(getPos()); } <APPLY>
            e2 = TableRef2(true) {
                if (!this.conformance.isApplyAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.applyNotAllowed());
                }
                e = new SqlJoin(joinType.getParserPosition(),
                    e,
                    SqlLiteral.createBoolean(false, joinType.getParserPosition()),
                    joinType,
                    e2,
                    JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                    SqlLiteral.createBoolean(true, joinType.getParserPosition()));
            }
        )
    )*
    {
        return e;
    }
}

/**
 * Parses a table reference in a FROM clause, not lateral unless LATERAL
 * is explicitly specified.
 */
SqlNode TableRef() :
{
    final SqlNode e;
}
{
    e = TableRef2(false) { return e; }
}

/**
 * Parses a table reference in a FROM clause.
 */
SqlNode TableRef2(boolean lateral) :
{
    SqlNode tableRef;
    final SqlNode over;
    final SqlNode snapshot;
    final SqlNode match;
    SqlNodeList extendList = null;
    final SqlIdentifier alias;
    final Span s, s2;
    SqlNodeList args;
    SqlNode sample;
    boolean isBernoulli;
    SqlNumericLiteral samplePercentage;
    boolean isRepeatable = false;
    int repeatableSeed = 0;
    SqlNodeList columnAliasList = null;
    SqlUnnestOperator unnestOp = SqlStdOperatorTable.UNNEST;
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
        LOOKAHEAD(2)
        [ <LATERAL> { lateral = true; } ]
        tableRef = ParenthesizedExpression(ExprContext.ACCEPT_QUERY)
        over = TableOverOpt()
        {
            if (over != null) {
                tableRef = SqlStdOperatorTable.OVER.createCall(
                    getPos(), tableRef, over);
            }
            if (lateral) {
                tableRef = SqlStdOperatorTable.LATERAL.createCall(
                    getPos(), tableRef);
            }
        }
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
        [<LATERAL> { lateral = true; } ]
        <TABLE> { s = span(); } <LPAREN>
        tableRef = TableFunctionCall(s.pos())
        <RPAREN>
        {
            if (lateral) {
                tableRef = SqlStdOperatorTable.LATERAL.createCall(
                    s.end(this), tableRef);
            }
        }
    |
        tableRef = ExtendedTableRef()
    )
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
    [
        <TABLESAMPLE> { s2 = span(); }
        (
            <SUBSTITUTE> <LPAREN> sample = StringLiteral() <RPAREN>
            {
                String sampleName =
                    SqlLiteral.unchain(sample).getValueAs(String.class);
                SqlSampleSpec sampleSpec = SqlSampleSpec.createNamed(sampleName);
                final SqlLiteral sampleLiteral =
                    SqlLiteral.createSample(sampleSpec, s2.end(this));
                tableRef = SqlStdOperatorTable.TABLESAMPLE.createCall(
                    s2.add(tableRef).end(this), tableRef, sampleLiteral);
            }
        |
            (
                <BERNOULLI>
                {
                    isBernoulli = true;
                }
            |
                <SYSTEM>
                {
                    isBernoulli = false;
                }
            )
            <LPAREN> samplePercentage = UnsignedNumericLiteral() <RPAREN>
            [
                <REPEATABLE> <LPAREN> repeatableSeed = IntLiteral() <RPAREN>
                {
                    isRepeatable = true;
                }
            ]
            {
                final BigDecimal ONE_HUNDRED = BigDecimal.valueOf(100L);
                BigDecimal rate = samplePercentage.bigDecimalValue();
                if (rate.compareTo(BigDecimal.ZERO) < 0
                    || rate.compareTo(ONE_HUNDRED) > 0)
                {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.invalidSampleSize());
                }

                // Treat TABLESAMPLE(0) and TABLESAMPLE(100) as no table
                // sampling at all.  Not strictly correct: TABLESAMPLE(0)
                // should produce no output, but it simplifies implementation
                // to know that some amount of sampling will occur.
                // In practice values less than ~1E-43% are treated as 0.0 and
                // values greater than ~99.999997% are treated as 1.0
                float fRate = rate.divide(ONE_HUNDRED).floatValue();
                if (fRate > 0.0f && fRate < 1.0f) {
                    SqlSampleSpec tableSampleSpec =
                    isRepeatable
                        ? SqlSampleSpec.createTableSample(
                            isBernoulli, fRate, repeatableSeed)
                        : SqlSampleSpec.createTableSample(isBernoulli, fRate);

                    SqlLiteral tableSampleLiteral =
                        SqlLiteral.createSample(tableSampleSpec, s2.end(this));
                    tableRef = SqlStdOperatorTable.TABLESAMPLE.createCall(
                        s2.end(this), tableRef, tableSampleLiteral);
                }
            }
        )
    ]
    {
        return tableRef;
    }
}

SqlNodeList ExtendList() :
{
    final Span s;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ColumnType(list)
    (
        <COMMA> ColumnType(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void ColumnType(List<SqlNode> list) :
{
    SqlIdentifier name;
    SqlDataTypeSpec type;
    boolean nullable = true;
}
{
    name = CompoundIdentifier()
    type = DataType()
    [
        <NOT> <NULL> {
            nullable = false;
        }
    ]
    {
        list.add(name);
        list.add(type.withNullable(nullable));
    }
}

/**
 * Parses a compound identifier with optional type.
 */
void CompoundIdentifierType(List<SqlNode> list, List<SqlNode> extendList) :
{
    final SqlIdentifier name;
    SqlDataTypeSpec type = null;
    boolean nullable = true;
}
{
    name = CompoundIdentifier()
    [
        type = DataType() {
            if (!this.conformance.allowExtend()) {
                throw SqlUtil.newContextException(getPos(), RESOURCE.extendNotAllowed());
            }
        }
        [
            <NOT> <NULL> {
                nullable = false;
            }
        ]
    ]
    {
       if (type != null) {
           extendList.add(name);
           extendList.add(type.withNullable(nullable));
       }
       list.add(name);
    }
}

SqlNode TableFunctionCall(SqlParserPos pos) :
{
    SqlNode call;
    SqlFunctionCategory funcType = SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
}
{
    [
        <SPECIFIC>
        {
            funcType = SqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION;
        }
    ]
    call = NamedRoutineCall(funcType, ExprContext.ACCEPT_CURSOR)
    {
        return SqlStdOperatorTable.COLLECTION_TABLE.createCall(pos, call);
    }
}

/**
 * Abstract production:
 *    SqlNode ExtendedTableRef()
 *
 * <p>Allows parser to be extended with new types of table references.  The
 * default implementation of this production is empty.
 */

/*
 * Abstract production:
 *
 *    SqlNode TableOverOpt()
 *
 * Allows an OVER clause following a table expression as an extension to
 * standard SQL syntax. The default implementation of this production is empty.
 */

/**
 * Parses an explicit TABLE t reference.
 */
SqlNode ExplicitTable(SqlParserPos pos) :
{
    SqlNode tableRef;
}
{
    <TABLE> tableRef = CompoundIdentifier()
    {
        return SqlStdOperatorTable.EXPLICIT_TABLE.createCall(pos, tableRef);
    }
}

/**
 * Parses a VALUES leaf query expression.
 */
SqlNode TableConstructor() :
{
    SqlNodeList rowConstructorList;
    final Span s;
}
{
    <VALUES> { s = span(); }
    rowConstructorList = RowConstructorList(s)
    {
        return SqlStdOperatorTable.VALUES.createCall(
            s.end(this), rowConstructorList.toArray());
    }
}

/**
 * Parses one or more rows in a VALUES expression.
 */
SqlNodeList RowConstructorList(Span s) :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode rowConstructor;
}
{
    rowConstructor = RowConstructor() { list.add(rowConstructor); }
    (
        LOOKAHEAD(2)
        <COMMA> rowConstructor = RowConstructor() { list.add(rowConstructor); }
    )*
    {
        return new SqlNodeList(list, s.end(this));
    }
}

/**
 * Parses a row constructor in the context of a VALUES expression.
 */
SqlNode RowConstructor() :
{
    SqlNodeList valueList;
    SqlNode value;
    final Span s;
}
{
    // hints are necessary here due to common LPAREN prefixes
    (
        // TODO jvs 8-Feb-2004: extra parentheses are accepted here as a hack
        // for unparse, but this is actually invalid SQL; should
        // fix unparse
        LOOKAHEAD(3)
        <LPAREN> { s = span(); }
        <ROW>
        valueList = ParenthesizedQueryOrCommaListWithDefault(ExprContext.ACCEPT_NONCURSOR)
        <RPAREN> { s.add(this); }
    |
        LOOKAHEAD(3)
        (
            <ROW> { s = span(); }
        |
            { s = Span.of(); }
        )
        valueList = ParenthesizedQueryOrCommaListWithDefault(ExprContext.ACCEPT_NONCURSOR)
    |
        value = Expression(ExprContext.ACCEPT_NONCURSOR)
        {
            // NOTE: A bare value here is standard SQL syntax, believe it or
            // not.  Taken together with multi-row table constructors, it leads
            // to very easy mistakes if you forget the parentheses on a
            // single-row constructor.  This is also the reason for the
            // LOOKAHEAD in RowConstructorList().  It would be so much more
            // reasonable to require parentheses.  Sigh.
            s = Span.of(value);
            valueList = new SqlNodeList(Collections.singletonList(value),
                value.getParserPosition());
        }
    )
    {
        // REVIEW jvs 8-Feb-2004: Should we discriminate between scalar
        // sub-queries inside of ROW and row sub-queries?  The standard does,
        // but the distinction seems to be purely syntactic.
        return SqlStdOperatorTable.ROW.createCall(s.end(valueList),
            valueList.toArray());
    }
}

/**
 * Parses the optional WHERE clause for SELECT, DELETE, and UPDATE.
 */
SqlNode WhereOpt() :
{
    SqlNode condition;
}
{
    <WHERE> condition = Expression(ExprContext.ACCEPT_SUB_QUERY)
    {
        return condition;
    }
    |
    {
        return null;
    }
}

/**
 * Parses the optional GROUP BY clause for SELECT.
 */
SqlNodeList GroupByOpt() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    final Span s;
}
{
    <GROUP> { s = span(); }
    <BY> list = GroupingElementList() {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

List<SqlNode> GroupingElementList() :
{
    List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    e = GroupingElement() { list.add(e); }
    (
        LOOKAHEAD(2)
        <COMMA>
        e = GroupingElement() { list.add(e); }
    )*
    { return list; }
}

SqlNode GroupingElement() :
{
    List<SqlNode> list;
    final SqlNodeList nodes;
    final SqlNode e;
    final Span s;
}
{
    LOOKAHEAD(2)
    <GROUPING> { s = span(); }
    <SETS> <LPAREN> list = GroupingElementList() <RPAREN> {
        return SqlStdOperatorTable.GROUPING_SETS.createCall(s.end(this), list);
    }
|   <ROLLUP> { s = span(); }
    <LPAREN> nodes = ExpressionCommaList(s, ExprContext.ACCEPT_SUB_QUERY)
    <RPAREN> {
        return SqlStdOperatorTable.ROLLUP.createCall(s.end(this),
            nodes.getList());
    }
|   <CUBE> { s = span(); }
    <LPAREN> nodes = ExpressionCommaList(s, ExprContext.ACCEPT_SUB_QUERY)
    <RPAREN> {
        return SqlStdOperatorTable.CUBE.createCall(s.end(this),
            nodes.getList());
    }
|   LOOKAHEAD(3)
    <LPAREN> <RPAREN> {
        return new SqlNodeList(getPos());
    }
|   e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        return e;
    }
}

/**
 * Parses a list of expressions separated by commas.
 */
SqlNodeList ExpressionCommaList(
    final Span s,
    ExprContext exprContext) :
{
    List<SqlNode> list;
    SqlNode e;
}
{
    e = Expression(exprContext)
    {
        list = startList(e);
    }
    (
        // NOTE jvs 6-Feb-2004:  See comments at top of file for why
        // hint is necessary here.
        LOOKAHEAD(2)
        <COMMA> e = Expression(ExprContext.ACCEPT_SUB_QUERY)
        {
            list.add(e);
        }
    )*
    {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
}

/**
 * Parses the optional HAVING clause for SELECT.
 */
SqlNode HavingOpt() :
{
    SqlNode e;
}
{
    <HAVING> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { return e; }
|
    { return null; }
}

/**
 * Parses the optional WINDOW clause for SELECT
 */
SqlNodeList WindowOpt() :
{
    SqlIdentifier id;
    SqlWindow e;
    List<SqlNode> list;
    final Span s;
}
{
    <WINDOW> { s = span(); }
    id = SimpleIdentifier() <AS> e = WindowSpecification() {
        e.setDeclName(id);
        list = startList(e);
    }
    (
        // NOTE jhyde 22-Oct-2004:  See comments at top of file for why
        // hint is necessary here.
        LOOKAHEAD(2)
        <COMMA> id = SimpleIdentifier() <AS> e = WindowSpecification() {
            e.setDeclName(id);
            list.add(e);
        }
    )*
    {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
|
    {
        return null;
    }
}

/**
 * Parses a window specification.
 */
SqlWindow WindowSpecification() :
{
    SqlIdentifier id;
    List list;
    SqlNodeList partitionList;
    SqlNodeList orderList;
    SqlLiteral isRows = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
    SqlNode lowerBound = null, upperBound = null;
    SqlParserPos startPos;
    final Span s, s1, s2;
    SqlLiteral allowPartial = null;
}
{
    <LPAREN> { s = span(); }
    (
        id = SimpleIdentifier()
    |
        { id = null; }
    )
    (
        <PARTITION> { s1 = span(); }
        <BY>
        partitionList = ExpressionCommaList(s1, ExprContext.ACCEPT_NON_QUERY)
    |
        { partitionList = SqlNodeList.EMPTY; }
    )
    (
        orderList = OrderBy(true)
    |
        { orderList = SqlNodeList.EMPTY; }
    )
    [
        (
            <ROWS> { isRows = SqlLiteral.createBoolean(true, getPos()); }
        |
            <RANGE> { isRows = SqlLiteral.createBoolean(false, getPos()); }
        )
        (
            <BETWEEN> lowerBound = WindowRange()
            <AND> upperBound = WindowRange()
        |
            lowerBound = WindowRange()
        )
    ]
    [
        <ALLOW> { s2 = span(); } <PARTIAL> {
            allowPartial = SqlLiteral.createBoolean(true, s2.end(this));
        }
    |
        <DISALLOW> { s2 = span(); } <PARTIAL> {
            allowPartial = SqlLiteral.createBoolean(false, s2.end(this));
        }
    ]
    <RPAREN>
    {
        return SqlWindow.create(
            null, id, partitionList, orderList,
            isRows, lowerBound, upperBound, allowPartial, s.end(this));
    }
}

SqlNode WindowRange() :
{
    final SqlNode e;
    final Span s;
}
{
    LOOKAHEAD(2)
    <CURRENT> { s = span(); } <ROW> {
        return SqlWindow.createCurrentRow(s.end(this));
    }
|
    LOOKAHEAD(2)
    <UNBOUNDED> { s = span(); }
    (
        <PRECEDING> {
            return SqlWindow.createUnboundedPreceding(s.end(this));
        }
    |
        <FOLLOWING> {
            return SqlWindow.createUnboundedFollowing(s.end(this));
        }
    )
|
    e = Expression(ExprContext.ACCEPT_NON_QUERY)
    (
        <PRECEDING> {
            return SqlWindow.createPreceding(e, getPos());
        }
    |
        <FOLLOWING> {
            return SqlWindow.createFollowing(e, getPos());
        }
    )
}

/**
 * Parses an ORDER BY clause.
 */
SqlNodeList OrderBy(boolean accept) :
{
    List<SqlNode> list;
    SqlNode e;
    final Span s;
}
{
    <ORDER> {
        s = span();
        if (!accept) {
            // Someone told us ORDER BY wasn't allowed here.  So why
            // did they bother calling us?  To get the correct
            // parser position for error reporting.
            throw SqlUtil.newContextException(s.pos(), RESOURCE.illegalOrderBy());
        }
    }
    <BY> e = OrderItem() {
        list = startList(e);
    }
    (
        // NOTE jvs 6-Feb-2004:  See comments at top of file for why
        // hint is necessary here.
        LOOKAHEAD(2) <COMMA> e = OrderItem() { list.add(e); }
    )*
    {
        return new SqlNodeList(list, s.addAll(list).pos());
    }
}

/**
 * Parses one list item in an ORDER BY clause.
 */
SqlNode OrderItem() :
{
    SqlNode e;
}
{
    e = Expression(ExprContext.ACCEPT_SUB_QUERY)
    (
        <ASC>
    |   <DESC> {
            e = SqlStdOperatorTable.DESC.createCall(getPos(), e);
        }
    )?
    (
        LOOKAHEAD(2)
        <NULLS> <FIRST> {
            e = SqlStdOperatorTable.NULLS_FIRST.createCall(getPos(), e);
        }
    |
        <NULLS> <LAST> {
            e = SqlStdOperatorTable.NULLS_LAST.createCall(getPos(), e);
        }
    )?
    {
        return e;
    }
}

/**
 * Parses a FOR SYSTEM_TIME clause following a table expression.
 */
SqlSnapshot Snapshot(SqlNode tableRef) :
{
    final Span s;
    final SqlNode e;
}
{
    { s = span(); } <FOR> <SYSTEM_TIME> <AS> <OF>
    // Syntax for temporal table in
    // standard SQL 2011 IWD 9075-2:201?(E) 7.6 <table reference>
    // supports grammar as following:
    // 1. datetime literal
    // 2. datetime value function, i.e. CURRENT_TIMESTAMP
    // 3. datetime term in 1 or 2 +(or -) interval term

    // We extend to support column reference, use Expression
    // to simplify the parsing code.
    e = Expression(ExprContext.ACCEPT_NON_QUERY) {
        return new SqlSnapshot(s.end(this), tableRef, e);
    }
}

/**
 * Parses a MATCH_RECOGNIZE clause following a table expression.
 */
SqlMatchRecognize MatchRecognize(SqlNode tableRef) :
{
    final Span s, s0, s1, s2;
    SqlNodeList measureList = SqlNodeList.EMPTY;
    SqlNodeList partitionList = SqlNodeList.EMPTY;
    SqlNodeList orderList = SqlNodeList.EMPTY;
    SqlNode pattern;
    SqlLiteral interval;
    SqlNodeList patternDefList;
    final SqlNode after;
    SqlParserPos pos;
    final SqlNode var;
    final SqlLiteral rowsPerMatch;
    SqlNodeList subsetList = SqlNodeList.EMPTY;
    SqlLiteral isStrictStarts = SqlLiteral.createBoolean(false, getPos());
    SqlLiteral isStrictEnds = SqlLiteral.createBoolean(false, getPos());
}
{
    <MATCH_RECOGNIZE> { s = span(); } <LPAREN>
    [
        <PARTITION> { s2 = span(); } <BY>
        partitionList = ExpressionCommaList(s2, ExprContext.ACCEPT_NON_QUERY)
    ]
    [
        orderList = OrderBy(true)
    ]
    [
        <MEASURES>
        measureList = MeasureColumnCommaList(span())
    ]
    (
        <ONE> { s0 = span(); } <ROW> <PER> <MATCH> {
            rowsPerMatch = SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(s0.end(this));
        }
    |
        <ALL> { s0 = span(); } <ROWS> <PER> <MATCH> {
            rowsPerMatch = SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(s0.end(this));
        }
    |
        {
            rowsPerMatch = null;
        }
    )
    (
        <AFTER> { s1 = span(); } <MATCH> <SKIP_>
        (
            <TO>
            (
                LOOKAHEAD(2)
                <NEXT> <ROW> {
                    after = SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW
                        .symbol(s1.end(this));
                }
            |
                LOOKAHEAD(2)
                <FIRST> var = SimpleIdentifier() {
                    after = SqlMatchRecognize.SKIP_TO_FIRST.createCall(
                        s1.end(var), var);
                }
            |
                // This "LOOKAHEAD({true})" is a workaround for dialects that use option "LOOKAHEAD=2"
                // globally, JavaCC generates something like "LOOKAHEAD(2, [<LAST>] SimpleIdentifier())"
                // here. But the correct LOOKAHEAD should be
                // "LOOKAHEAD(2, [ LOOKAHEAD(2, <LAST> SimpleIdentifier()) <LAST> ]
                // SimpleIdentifier())" which have the syntactic lookahead for <LAST> considered.
                //
                // Overall LOOKAHEAD({true}) is even better as this is the last branch in the
                // choice.
                LOOKAHEAD({true})
                [ LOOKAHEAD(2, <LAST> SimpleIdentifier()) <LAST> ] var = SimpleIdentifier() {
                    after = SqlMatchRecognize.SKIP_TO_LAST.createCall(
                        s1.end(var), var);
                }
            )
        |
            <PAST> <LAST> <ROW> {
                 after = SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW
                     .symbol(s1.end(this));
            }
        )
    |
        { after = null; }
    )
    <PATTERN>
    <LPAREN>
    (
        <CARET> { isStrictStarts = SqlLiteral.createBoolean(true, getPos()); }
    |
        { isStrictStarts = SqlLiteral.createBoolean(false, getPos()); }
    )
    pattern = PatternExpression()
    (
        <DOLLAR> { isStrictEnds = SqlLiteral.createBoolean(true, getPos()); }
    |
        { isStrictEnds = SqlLiteral.createBoolean(false, getPos()); }
    )
    <RPAREN>
    (
        <WITHIN> interval = IntervalLiteral()
    |
        { interval = null; }
    )
    [
        <SUBSET>
        subsetList = SubsetDefinitionCommaList(span())
    ]
    <DEFINE>
    patternDefList = PatternDefinitionCommaList(span())
    <RPAREN> {
        return new SqlMatchRecognize(s.end(this), tableRef,
            pattern, isStrictStarts, isStrictEnds, patternDefList, measureList,
            after, subsetList, rowsPerMatch, partitionList, orderList, interval);
    }
}

SqlNodeList MeasureColumnCommaList(Span s) :
{
    SqlNode e;
    final List<SqlNode> eList = new ArrayList<SqlNode>();
}
{
    e = MeasureColumn() {
        eList.add(e);
    }
    (
        <COMMA>
        e = MeasureColumn() {
            eList.add(e);
        }
    )*
    {
        return new SqlNodeList(eList, s.addAll(eList).pos());
    }
}

SqlNode MeasureColumn() :
{
    SqlNode e;
    SqlIdentifier alias;
}
{
    e = Expression(ExprContext.ACCEPT_NON_QUERY)
    <AS>
    alias = SimpleIdentifier() {
        return SqlStdOperatorTable.AS.createCall(Span.of(e).end(this), e, alias);
    }
}

SqlNode PatternExpression() :
{
    SqlNode left;
    SqlNode right;
}
{
    left = PatternTerm()
    (
        <VERTICAL_BAR>
        right = PatternTerm() {
            left = SqlStdOperatorTable.PATTERN_ALTER.createCall(
                Span.of(left).end(right), left, right);
        }
    )*
    {
        return left;
    }
}

SqlNode PatternTerm() :
{
    SqlNode left;
    SqlNode right;
}
{
    left = PatternFactor()
    (
        right = PatternFactor() {
            left = SqlStdOperatorTable.PATTERN_CONCAT.createCall(
                Span.of(left).end(right), left, right);
        }
    )*
    {
        return left;
    }
}

SqlNode PatternFactor() :
{
    SqlNode e;
    SqlNode extra;
    SqlLiteral startNum = null;
    SqlLiteral endNum = null;
    SqlLiteral reluctant = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
}
{
    e = PatternPrimary()
    [
        LOOKAHEAD(1)
        (
            <STAR> {
                startNum = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
                endNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
            }
        |
            <PLUS> {
                startNum = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
                endNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
            }
        |
            <HOOK> {
                startNum = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
                endNum = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
            }
        |
            <LBRACE>
            (
                startNum = UnsignedNumericLiteral() { endNum = startNum; }
                [
                    <COMMA> {
                        endNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
                    }
                    [
                        endNum = UnsignedNumericLiteral()
                    ]
                ]
                <RBRACE>
            |
                {
                    startNum = SqlLiteral.createExactNumeric("-1", SqlParserPos.ZERO);
                }
                <COMMA>
                endNum = UnsignedNumericLiteral()
                <RBRACE>
            |
                <MINUS> extra = PatternExpression() <MINUS> <RBRACE> {
                    extra = SqlStdOperatorTable.PATTERN_EXCLUDE.createCall(
                        Span.of(extra).end(this), extra);
                    e = SqlStdOperatorTable.PATTERN_CONCAT.createCall(
                        Span.of(e).end(this), e, extra);
                    return e;
                }
            )
        )
        [
            <HOOK>
            {
                if (startNum.intValue(true) != endNum.intValue(true)) {
                    reluctant = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
                }
            }
        ]
    ]
    {
        if (startNum == null) {
            return e;
        } else {
            return SqlStdOperatorTable.PATTERN_QUANTIFIER.createCall(
                span().end(e), e, startNum, endNum, reluctant);
        }
    }
}

SqlNode PatternPrimary() :
{
    final Span s;
    SqlNode e;
    List<SqlNode> eList;
}
{
    (
        e = SimpleIdentifier()
    |
        <LPAREN> e = PatternExpression() <RPAREN>
    |
        <LBRACE> { s = span(); }
        <MINUS> e = PatternExpression()
        <MINUS> <RBRACE> {
            e = SqlStdOperatorTable.PATTERN_EXCLUDE.createCall(s.end(this), e);
        }
    |
        (
            <PERMUTE> { s = span(); }
            <LPAREN>
            e = PatternExpression() {
                eList = new ArrayList<SqlNode>();
                eList.add(e);
            }
            (
                <COMMA>
                e = PatternExpression()
                {
                    eList.add(e);
                }
            )*
            <RPAREN> {
                e = SqlStdOperatorTable.PATTERN_PERMUTE.createCall(
                    s.end(this), eList);
            }
        )
    )
    {
        return e;
    }
}

SqlNodeList SubsetDefinitionCommaList(Span s) :
{
    SqlNode e;
    final List<SqlNode> eList = new ArrayList<SqlNode>();
}
{
    e = SubsetDefinition() {
        eList.add(e);
    }
    (
        <COMMA>
        e = SubsetDefinition() {
            eList.add(e);
        }
    )*
    {
        return new SqlNodeList(eList, s.addAll(eList).pos());
    }
}

SqlNode SubsetDefinition() :
{
    final SqlNode var;
    final SqlNodeList varList;
}
{
    var = SimpleIdentifier()
    <EQ>
    <LPAREN>
    varList = ExpressionCommaList(span(), ExprContext.ACCEPT_NON_QUERY)
    <RPAREN> {
        return SqlStdOperatorTable.EQUALS.createCall(span().end(var), var,
            varList);
    }
}

SqlNodeList PatternDefinitionCommaList(Span s) :
{
    SqlNode e;
    final List<SqlNode> eList = new ArrayList<SqlNode>();
}
{
    e = PatternDefinition() {
        eList.add(e);
    }
    (
        <COMMA>
        e = PatternDefinition() {
            eList.add(e);
        }
    )*
    {
        return new SqlNodeList(eList, s.addAll(eList).pos());
    }
}

SqlNode PatternDefinition() :
{
    final SqlNode var;
    final SqlNode e;
}
{
    var = SimpleIdentifier()
    <AS>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        return SqlStdOperatorTable.AS.createCall(Span.of(var, e).pos(), e, var);
    }
}

// ----------------------------------------------------------------------------
// Expressions

/**
 * Parses a SQL expression (such as might occur in a WHERE clause) followed by
 * the end-of-file symbol.
 */
SqlNode SqlExpressionEof() :
{
    SqlNode e;
}
{
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) (<EOF>)
    {
        return e;
    }
}

/**
 * Parses either a row expression or a query expression without ORDER BY.
 */
SqlNode QueryOrExpr(ExprContext exprContext) :
{
    SqlNodeList withList = null;
    SqlNode e;
    SqlOperator op;
    SqlParserPos pos;
    SqlParserPos withPos;
    List<Object> list;
}
{
    [
        withList = WithList()
    ]
    e = LeafQueryOrExpr(exprContext) {
        list = startList(e);
    }
    (
        {
            if (!e.isA(SqlKind.QUERY)) {
                // whoops, expression we just parsed wasn't a query,
                // but we're about to see something like UNION, so
                // force an exception retroactively
                checkNonQueryExpression(ExprContext.ACCEPT_QUERY);
            }
        }
        op = BinaryQueryOperator() {
            // ensure a query is legal in this context
            pos = getPos();
            checkQueryExpression(exprContext);

        }
        e = LeafQueryOrExpr(ExprContext.ACCEPT_QUERY) {
            list.add(new SqlParserUtil.ToTreeListItem(op, pos));
            list.add(e);
        }
    )*
    {
        e = SqlParserUtil.toTree(list);
        if (withList != null) {
            e = new SqlWith(withList.getParserPosition(), withList, e);
        }
        return e;
    }
}

SqlNodeList WithList() :
{
    SqlWithItem withItem;
    SqlParserPos pos;
    SqlNodeList list;
}
{
    <WITH> { list = new SqlNodeList(getPos()); }
    withItem = WithItem() {list.add(withItem);}
    (
        <COMMA> withItem = WithItem() {list.add(withItem);}
    )*
    { return list; }
}

SqlWithItem WithItem() :
{
    SqlIdentifier id;
    SqlNodeList columnList = null;
    SqlNode definition;
}
{
    id = SimpleIdentifier()
    [
        LOOKAHEAD(2)
        columnList = ParenthesizedSimpleIdentifierList()
    ]
    <AS>
    definition = ParenthesizedExpression(ExprContext.ACCEPT_QUERY)
    {
        return new SqlWithItem(id.getParserPosition(), id, columnList,
            definition);
    }
}

/**
 * Parses either a row expression, a leaf query expression, or
 * a parenthesized expression of any kind.
 */
SqlNode LeafQueryOrExpr(ExprContext exprContext) :
{
    SqlNode e;
}
{
    e = Expression(exprContext) { return e; }
|
    e = LeafQuery(exprContext) { return e; }
}

/**
 * Parses a row expression or a parenthesized expression of any kind.
 */
SqlNode Expression(ExprContext exprContext) :
{
    List<Object> list;
    SqlNode e;
}
{
    list = Expression2(exprContext)
    {
        e = SqlParserUtil.toTree(list);
        return e;
    }
}

// TODO jvs 15-Nov-2003:  ANY/ALL

void Expression2b(ExprContext exprContext, List<Object> list) :
{
    SqlNode e;
    SqlOperator op;
    SqlNode ext;
}
{
    (
        LOOKAHEAD(1)
        op = PrefixRowOperator() {
            checkNonQueryExpression(exprContext);
            list.add(new SqlParserUtil.ToTreeListItem(op, getPos()));
        }
    )*
    e = Expression3(exprContext) {
        list.add(e);
    }
    (
        LOOKAHEAD(2) <DOT>
        ext = RowExpressionExtension() {
            list.add(
                new SqlParserUtil.ToTreeListItem(
                    SqlStdOperatorTable.DOT, getPos()));
            list.add(ext);
        }
    )*
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
                LOOKAHEAD(2) {
                    checkNonQueryExpression(exprContext);
                }
                (
                    <NOT> <IN> { op = SqlStdOperatorTable.NOT_IN; }
                |
                    <IN> { op = SqlStdOperatorTable.IN; }
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
<#if parser.includeLikeAnyAllSome>
                    LOOKAHEAD(3)
                    LikeAnyAllSome(list, s)
                |
</#if>
                    (
                        (
                            <NOT>
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
                    <#if parser.includePosixOperators>
                    |
                        <NEGATE> <TILDE> { op = SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_SENSITIVE; }
                        [ <STAR> { op = SqlStdOperatorTable.NEGATED_POSIX_REGEX_CASE_INSENSITIVE; } ]
                    |
                        <TILDE> { op = SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE; }
                        [ <STAR> { op = SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE; } ]
                    </#if>
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
            <#list parser.extraBinaryExpressions as extra >
                ${extra}(list, exprContext, s)
            |
            </#list>
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

/** Parses a comparison operator inside a SOME / ALL predicate. */
SqlKind comp() :
{
}
{
    <LT> { return SqlKind.LESS_THAN; }
|
    <LE> { return SqlKind.LESS_THAN_OR_EQUAL; }
|
    <GT> { return SqlKind.GREATER_THAN; }
|
    <GE> { return SqlKind.GREATER_THAN_OR_EQUAL; }
|
    <EQ> { return SqlKind.EQUALS; }
|
    <NE> { return SqlKind.NOT_EQUALS; }
|
    <NE2> {
        if (!this.conformance.isBangEqualAllowed()) {
            throw SqlUtil.newContextException(getPos(), RESOURCE.bangEqualNotAllowed());
        }
        return SqlKind.NOT_EQUALS;
    }
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
    Span rowSpan = null;
}
{
<#list parser.preExpressionMethods as method>
    LOOKAHEAD(${method}())
    e = ${method}() { return e; }
|
</#list>
    LOOKAHEAD(2)
    e = AtomicRowExpression()
    {
        checkNonQueryExpression(exprContext);
        return e;
    }
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
<#list parser.postExpressionMethods as method>
        e = ${method}(list1.get(0)) { return e; }
    |
</#list>
        { return list1.get(0); }
    )
}

SqlOperator periodOperator() :
{
}
{
     <OVERLAPS> { return SqlStdOperatorTable.OVERLAPS; }
|
     LOOKAHEAD(2)
     <IMMEDIATELY> <PRECEDES> { return SqlStdOperatorTable.IMMEDIATELY_PRECEDES; }
|
     <PRECEDES> { return SqlStdOperatorTable.PRECEDES; }
|
     <IMMEDIATELY> <SUCCEEDS> { return SqlStdOperatorTable.IMMEDIATELY_SUCCEEDS; }
|
     <SUCCEEDS> { return SqlStdOperatorTable.SUCCEEDS; }
|
     <EQUALS> { return SqlStdOperatorTable.PERIOD_EQUALS; }
}

/**
 * Parses a COLLATE clause
 */
SqlCollation CollateClause() :
{
}
{
    <COLLATE> <COLLATION_ID>
    {
        return new SqlCollation(
            getToken(0).image, SqlCollation.Coercibility.EXPLICIT);
    }
}

/**
 * Numeric literal or parameter; used in LIMIT, OFFSET and FETCH clauses.
 */
SqlNode UnsignedNumericLiteralOrParam() :
{
    final SqlNode e;
}
{
    (
        e = UnsignedNumericLiteral()
    |
        e = DynamicParam()
    )
    { return e; }
}

/**
 * Parses a row expression extension, it can be either an identifier,
 * or a call to a named function.
 */
SqlNode RowExpressionExtension() :
{
    final SqlFunctionCategory funcType;
    final SqlIdentifier p;
    final Span s;
    final List<SqlNode> args;
    SqlCall call;
    SqlNode e;
    SqlLiteral quantifier = null;
}
{
    p = SimpleIdentifier() {
        e = p;
    }
    (
        LOOKAHEAD( <LPAREN> ) { s = span(); }
        {
            funcType = SqlFunctionCategory.USER_DEFINED_FUNCTION;
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
            call = createCall(p, s.end(this), funcType, quantifier, args);
            e = call;
        }
    )?
    {
        return e;
    }
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
<#list parser.dateTimeExpressionMethods as method>
        LOOKAHEAD(${method})
        e = ${method}
    |
</#list>
<#if parser.includeHostVariables>
        e = SqlHostVariable()
    |
</#if>
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

SqlNode CaseExpression() :
{
    final Span whenSpan = Span.of();
    final Span thenSpan = Span.of();
    final Span s;
    SqlNode e;
    SqlNode caseIdentifier = null;
    SqlNode elseClause = null;
    List<SqlNode> whenList = new ArrayList<SqlNode>();
    List<SqlNode> thenList = new ArrayList<SqlNode>();
}
{
    <CASE> { s = span(); }
    [
        caseIdentifier = Expression(ExprContext.ACCEPT_SUB_QUERY)
    ]
    (
        <WHEN> { whenSpan.add(this); }
        e = ExpressionCommaList(s, ExprContext.ACCEPT_SUB_QUERY) {
            if (((SqlNodeList) e).size() == 1) {
                e = ((SqlNodeList) e).get(0);
            }
            whenList.add(e);
        }
        <THEN> { thenSpan.add(this); }
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            thenList.add(e);
        }
    )+
    [
        <ELSE> elseClause = Expression(ExprContext.ACCEPT_SUB_QUERY)
    ]
    <END> {
        return SqlCase.createSwitched(s.end(this), caseIdentifier,
            new SqlNodeList(whenList, whenSpan.addAll(whenList).pos()),
            new SqlNodeList(thenList, thenSpan.addAll(thenList).pos()),
            elseClause);
    }
}

SqlCall SequenceExpression() :
{
    final Span s;
    final SqlOperator f;
    final SqlNode sequenceRef;
}
{
    (
        <NEXT> { f = SqlStdOperatorTable.NEXT_VALUE; s = span(); }
    |
        LOOKAHEAD(3)
        <CURRENT> { f = SqlStdOperatorTable.CURRENT_VALUE; s = span(); }
    )
    <VALUE> <FOR> sequenceRef = CompoundIdentifier() {
        return f.createCall(s.end(sequenceRef), sequenceRef);
    }
}

/**
 * Parses "SET &lt;NAME&gt; = VALUE" or "RESET &lt;NAME&gt;", without a leading
 * "ALTER &lt;SCOPE&gt;".
 */
SqlSetOption SqlSetOption(Span s, String scope) :
{
    SqlIdentifier name;
    final SqlNode val;
}
{
    (
        <SET> {
            s.add(this);
        }
        name = CompoundIdentifier()
        <EQ>
        (
            val = Literal()
        |
            val = SimpleIdentifier()
        |
            <ON> {
                // OFF is handled by SimpleIdentifier, ON handled here.
                val = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT),
                    getPos());
            }
        )
        {
            return new SqlSetOption(s.end(val), scope, name, val);
        }
    |
        <RESET> {
            s.add(this);
        }
        (
            name = CompoundIdentifier()
        |
            <ALL> {
                name = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT),
                    getPos());
            }
        )
        {
            return new SqlSetOption(s.end(name), scope, name, null);
        }
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
<#-- additional literal parser methods are included here -->
<#list parser.alterStatementParserMethods as method>
        alterNode = ${method}(s, scope)
    |
</#list>

        alterNode = SqlSetOption(s, scope)
    )
    {
        return alterNode;
    }
}

String Scope() :
{
}
{
    ( <SYSTEM> | <SESSION> ) { return token.image.toUpperCase(Locale.ROOT); }
}

<#if parser.createStatementParserMethods?size != 0>
/**
 * Parses a CREATE statement.
 */
SqlCreate SqlCreate() :
{
    final SqlCreate create;
}
{
    (
<#-- additional literal parser methods are included here -->
<#list parser.createStatementParserMethods as method>
        LOOKAHEAD(4)
        create = ${method}()
        <#sep>|</#sep>
</#list>
    )
    {
        return create;
    }
}
</#if>
<#if parser.setTimeZoneStatementParserMethods?size != 0>
/**
 * Parses a SET TIME ZONE statement
 */
SqlNode SqlSetTimeZone() :
{
    SqlNode source;
}
{
    (
<#-- additional literal parser methods are included here -->
<#list parser.setTimeZoneStatementParserMethods as method>
        source = ${method}()
        <#sep>|</#sep>
</#list>
    )
    {
        return source;
    }
}
</#if>
<#if parser.allowUpsertFormOfUpdate>
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
</#if>
<#if parser.renameStatementParserMethods?size != 0>
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
<#-- additional literal parser methods are included here -->
<#list parser.renameStatementParserMethods as method>
        source = ${method}()
        <#sep>|</#sep>
</#list>
    )
    {
        return source;
    }
}
</#if>
<#if parser.execStatementParserMethods?size != 0>
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
    (
<#-- additional literal parser methods are included here -->
<#list parser.execStatementParserMethods as method>
        source =  ${method}()
        <#sep>|</#sep>
</#list>
    )
    {
        return source;
    }
}
</#if>
<#if parser.usingStatementParserMethods?size != 0>
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
    (
<#-- additional literal parser methods are included here -->
<#list parser.usingStatementParserMethods as method>
        source =  ${method}(s)
        <#sep>|</#sep>
</#list>
    )
    {
        return source;
    }
}
</#if>
<#if parser.dropStatementParserMethods?size != 0>
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
<#-- additional literal parser methods are included here -->
<#list parser.dropStatementParserMethods as method>
        drop = ${method}(s, replace)
        <#sep>|</#sep>
</#list>
    )
    {
        return drop;
    }
}
</#if>

/**
 * Parses a literal expression, allowing continued string literals.
 * Usually returns an SqlLiteral, but a continued string literal
 * is an SqlCall expression, which concatenates 2 or more string
 * literals; the validator reduces this.
 */
SqlNode Literal() :
{
    SqlNode e;
}
{
    (
        e = NumericLiteral()
    |
        e = StringLiteral()
    |
        e = SpecialLiteral()
    |
        e = DateTimeLiteral()
    |
        e = IntervalLiteral()
<#-- additional literal parser methods are included here -->
<#list parser.literalParserMethods as method>
    |
        e = ${method}
</#list>
    )
    {
        return e;
    }


}

/** Parses a unsigned numeric literal */
SqlNumericLiteral UnsignedNumericLiteral() :
{
}
{
    <UNSIGNED_INTEGER_LITERAL> {
        return SqlLiteral.createExactNumeric(token.image, getPos());
    }
|
    <DECIMAL_NUMERIC_LITERAL> {
        return SqlLiteral.createExactNumeric(token.image, getPos());
    }
|
    <APPROX_NUMERIC_LITERAL> {
        return SqlLiteral.createApproxNumeric(token.image, getPos());
    }
}

/** Parses a numeric literal (can be signed) */
SqlLiteral NumericLiteral() :
{
    final SqlNumericLiteral num;
    final Span s;
}
{
    <PLUS> num = UnsignedNumericLiteral() {
        return num;
    }
|
    <MINUS> { s = span(); } num = UnsignedNumericLiteral() {
        return SqlLiteral.createNegative(num, s.end(this));
    }
|
    num = UnsignedNumericLiteral() {
        return num;
    }
}

/** Parse a special literal keyword */
SqlLiteral SpecialLiteral() :
{
}
{
    <TRUE> { return SqlLiteral.createBoolean(true, getPos()); }
|
    <FALSE> { return SqlLiteral.createBoolean(false, getPos()); }
|
    <UNKNOWN> { return SqlLiteral.createUnknown(getPos()); }
|
    <NULL> { return SqlLiteral.createNull(getPos()); }
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
<#if parser.allowHexCharacterStringLiteral>
    SqlNode hexCharLiteral;
</#if>
}
{
    // A continued string literal consists of a head fragment and one or more
    // tail fragments. Since comments may occur between the fragments, and
    // comments are special tokens, each fragment is a token. But since spaces
    // or comments may not occur between the prefix and the first quote, the
    // head fragment, with any prefix, is one token.

<#if parser.allowHexCharacterStringLiteral>
    hexCharLiteral = SqlHexCharStringLiteral()
    { return hexCharLiteral; }
    |
</#if>
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
 * Parses a date/time literal.
 */
SqlLiteral DateTimeLiteral() :
{
    final String  p;
    final Span s;
}
{
    <LBRACE_D> <QUOTED_STRING> {
        p = token.image;
    }
    <RBRACE> {
        return SqlParserUtil.parseDateLiteral(p, getPos());
    }
|
    <LBRACE_T> <QUOTED_STRING> {
        p = token.image;
    }
    <RBRACE> {
        return SqlParserUtil.parseTimeLiteral(p, getPos());
    }
|
    <LBRACE_TS> { s = span(); } <QUOTED_STRING> {
        p = token.image;
    }
    <RBRACE> {
        return SqlParserUtil.parseTimestampLiteral(p, s.end(this));
    }
|
    <DATE> { s = span(); } <QUOTED_STRING> {
        return SqlParserUtil.parseDateLiteral(token.image, s.end(this));
    }
|
    <TIME> { s = span(); } <QUOTED_STRING> {
        return SqlParserUtil.parseTimeLiteral(token.image, s.end(this));
    }
|
    <TIMESTAMP> { s = span(); } <QUOTED_STRING> {
        return SqlParserUtil.parseTimestampLiteral(token.image, s.end(this));
    }
}

/** Parses a MULTISET constructor */
SqlNode MultisetConstructor() :
{
    List<SqlNode> args;
    SqlNode e;
    final Span s;
}
{
    <MULTISET> { s = span(); }
    (
        LOOKAHEAD(2)
        <LPAREN>
        // by sub query "MULTISET(SELECT * FROM T)"
        e = LeafQueryOrExpr(ExprContext.ACCEPT_QUERY)
        <RPAREN> {
            return SqlStdOperatorTable.MULTISET_QUERY.createCall(
                s.end(this), e);
        }
    |
        // by enumeration "MULTISET[e0, e1, ..., eN]"
        <LBRACKET> // TODO: do trigraph as well ??( ??)
        e = Expression(ExprContext.ACCEPT_NON_QUERY) { args = startList(e); }
        (
            <COMMA> e = Expression(ExprContext.ACCEPT_NON_QUERY) { args.add(e); }
        )*
        <RBRACKET>
        {
            return SqlStdOperatorTable.MULTISET_VALUE.createCall(
                s.end(this), args);
        }
    )
}

/** Parses an ARRAY constructor */
SqlNode ArrayConstructor() :
{
    SqlNodeList args;
    SqlNode e;
    final Span s;
}
{
    <ARRAY> { s = span(); }
    (
        LOOKAHEAD(1)
        <LPAREN>
        // by sub query "MULTISET(SELECT * FROM T)"
        e = LeafQueryOrExpr(ExprContext.ACCEPT_QUERY)
        <RPAREN>
        {
            return SqlStdOperatorTable.ARRAY_QUERY.createCall(
                s.end(this), e);
        }
    |
        // by enumeration "ARRAY[e0, e1, ..., eN]"
        <LBRACKET> // TODO: do trigraph as well ??( ??)
        (
            args = ExpressionCommaList(s, ExprContext.ACCEPT_NON_QUERY)
        |
            { args = SqlNodeList.EMPTY; }
        )
        <RBRACKET>
        {
            return SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR.createCall(
                s.end(this), args.getList());
        }
    )
}

/** Parses a MAP constructor */
SqlNode MapConstructor() :
{
    SqlNodeList args;
    SqlNode e;
    final Span s;
}
{
    <MAP> { s = span(); }
    (
        LOOKAHEAD(1)
        <LPAREN>
        // by sub query "MAP (SELECT empno, deptno FROM emp)"
        e = LeafQueryOrExpr(ExprContext.ACCEPT_QUERY)
        <RPAREN>
        {
            return SqlStdOperatorTable.MAP_QUERY.createCall(
                s.end(this), e);
        }
    |
        // by enumeration "MAP[k0, v0, ..., kN, vN]"
        <LBRACKET> // TODO: do trigraph as well ??( ??)
        (
            args = ExpressionCommaList(s, ExprContext.ACCEPT_NON_QUERY)
        |
            { args = SqlNodeList.EMPTY; }
        )
        <RBRACKET>
        {
            return SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR.createCall(
                s.end(this), args.getList());
        }
    )
}

/** Parses a PERIOD constructor */
SqlNode PeriodConstructor() :
{
    final SqlNode e0, e1;
    final Span s;
}
{
    <PERIOD> { s = span(); }
    <LPAREN>
    e0 = Expression(ExprContext.ACCEPT_SUB_QUERY)
    <COMMA>
    e1 = Expression(ExprContext.ACCEPT_SUB_QUERY)
    <RPAREN> {
        return SqlStdOperatorTable.ROW.createCall(s.end(this), e0, e1);
    }
}

/**
 * Parses an interval literal.
 */
SqlLiteral IntervalLiteral() :
{
    final String p;
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
    <QUOTED_STRING> { p = token.image; }
    intervalQualifier = IntervalQualifier() {
        return SqlParserUtil.parseIntervalLiteral(s.end(intervalQualifier),
            sign, p, intervalQualifier);
    }
}

TimeUnit Year() :
{
}
{
    <YEAR> { return TimeUnit.YEAR; }
|
    <YEARS> { return warn(TimeUnit.YEAR); }
}

TimeUnit Month() :
{
}
{
    <MONTH> { return TimeUnit.MONTH; }
|
    <MONTHS> { return warn(TimeUnit.MONTH); }
}

TimeUnit Day() :
{
}
{
    <DAY> { return TimeUnit.DAY; }
|
    <DAYS> { return warn(TimeUnit.DAY); }
}

TimeUnit Hour() :
{
}
{
    <HOUR> { return TimeUnit.HOUR; }
|
    <HOURS> { return warn(TimeUnit.HOUR); }
}

TimeUnit Minute() :
{
}
{
    <MINUTE> { return TimeUnit.MINUTE; }
|
    <MINUTES> { return warn(TimeUnit.MINUTE); }
}

TimeUnit Second() :
{
}
{
    <SECOND> { return TimeUnit.SECOND; }
|
    <SECONDS> { return warn(TimeUnit.SECOND); }
}

SqlIntervalQualifier IntervalQualifier() :
{
    final TimeUnit start;
    TimeUnit end = null;
    int startPrec = RelDataType.PRECISION_NOT_SPECIFIED;
    int secondFracPrec = RelDataType.PRECISION_NOT_SPECIFIED;
}
{
    (
        start = Year() [ <LPAREN> startPrec = UnsignedIntLiteral() <RPAREN> ]
        [
            LOOKAHEAD(2) <TO> end = Month()
        ]
    |
        start = Month() [ <LPAREN> startPrec = UnsignedIntLiteral() <RPAREN> ]
    |
        start = Day() [ <LPAREN> startPrec = UnsignedIntLiteral() <RPAREN> ]
        [ LOOKAHEAD(2) <TO>
            (
                end = Hour()
            |
                end = Minute()
            |
                end = Second()
                [ <LPAREN> secondFracPrec = UnsignedIntLiteral() <RPAREN> ]
            )
        ]
    |
        start = Hour() [ <LPAREN> startPrec = UnsignedIntLiteral() <RPAREN> ]
        [ LOOKAHEAD(2) <TO>
            (
                end = Minute()
            |
                end = Second()
                [ <LPAREN> secondFracPrec = UnsignedIntLiteral() <RPAREN> ]
            )
        ]
    |
        start = Minute() [ <LPAREN> startPrec = UnsignedIntLiteral() <RPAREN> ]
        [ LOOKAHEAD(2) <TO>
            (
                end = Second()
                [ <LPAREN> secondFracPrec = UnsignedIntLiteral() <RPAREN> ]
            )
        ]
    |
        start = Second()
        [   <LPAREN> startPrec = UnsignedIntLiteral()
            [ <COMMA> secondFracPrec = UnsignedIntLiteral() ]
            <RPAREN>
        ]
    )
    {
        return new SqlIntervalQualifier(start,
            startPrec,
            end,
            secondFracPrec,
            getPos());
    }
}

/**
 * Parses time unit for EXTRACT, CEIL and FLOOR functions.
 * Note that it does't include NANOSECOND and MICROSECOND.
 */
TimeUnit TimeUnit() :
{}
{
    <MILLISECOND> { return TimeUnit.MILLISECOND; }
|   <SECOND> { return TimeUnit.SECOND; }
|   <MINUTE> { return TimeUnit.MINUTE; }
|   <HOUR> { return TimeUnit.HOUR; }
|   <DAY> { return TimeUnit.DAY; }
|   <DOW> { return TimeUnit.DOW; }
|   <DOY> { return TimeUnit.DOY; }
|   <ISODOW> { return TimeUnit.ISODOW; }
|   <ISOYEAR> { return TimeUnit.ISOYEAR; }
|   <WEEK> { return TimeUnit.WEEK; }
|   <MONTH> { return TimeUnit.MONTH; }
|   <QUARTER> { return TimeUnit.QUARTER; }
|   <YEAR> { return TimeUnit.YEAR; }
|   <EPOCH> { return TimeUnit.EPOCH; }
|   <DECADE> { return TimeUnit.DECADE; }
|   <CENTURY> { return TimeUnit.CENTURY; }
|   <MILLENNIUM> { return TimeUnit.MILLENNIUM; }
}

TimeUnit TimestampInterval() :
{}
{
    <FRAC_SECOND> { return TimeUnit.MICROSECOND; }
|   <MICROSECOND> { return TimeUnit.MICROSECOND; }
|   <NANOSECOND> { return TimeUnit.NANOSECOND; }
|   <SQL_TSI_FRAC_SECOND> { return TimeUnit.NANOSECOND; }
|   <SQL_TSI_MICROSECOND> { return TimeUnit.MICROSECOND; }
|   <SECOND> { return TimeUnit.SECOND; }
|   <SQL_TSI_SECOND> { return TimeUnit.SECOND; }
|   <MINUTE> { return TimeUnit.MINUTE; }
|   <SQL_TSI_MINUTE> { return TimeUnit.MINUTE; }
|   <HOUR> { return TimeUnit.HOUR; }
|   <SQL_TSI_HOUR> { return TimeUnit.HOUR; }
|   <DAY> { return TimeUnit.DAY; }
|   <SQL_TSI_DAY> { return TimeUnit.DAY; }
|   <WEEK> { return TimeUnit.WEEK; }
|   <SQL_TSI_WEEK> { return TimeUnit.WEEK; }
|   <MONTH> { return TimeUnit.MONTH; }
|   <SQL_TSI_MONTH> { return TimeUnit.MONTH; }
|   <QUARTER> { return TimeUnit.QUARTER; }
|   <SQL_TSI_QUARTER> { return TimeUnit.QUARTER; }
|   <YEAR> { return TimeUnit.YEAR; }
|   <SQL_TSI_YEAR> { return TimeUnit.YEAR; }
}



/**
 * Parses a dynamic parameter marker.
 */
SqlDynamicParam DynamicParam() :
{
}
{
    <HOOK> {
        return new SqlDynamicParam(nDynamicParams++, getPos());
    }
}

/**
 * Parses one segment of an identifier that may be composite.
 *
 * <p>Each time it reads an identifier it writes one element to each list;
 * the entry in {@code positions} records its position and whether the
 * segment was quoted.
 */
void IdentifierSegment(List<String> names, List<SqlParserPos> positions) :
{
    final String id;
    char unicodeEscapeChar = BACKSLASH;
    final SqlParserPos pos;
    final Span span;
}
{
    (
        <IDENTIFIER> {
            id = unquotedIdentifier();
            pos = getPos();
        }
    |
        <QUOTED_IDENTIFIER> {
            id = SqlParserUtil.strip(getToken(0).image, DQ, DQ, DQDQ,
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BACK_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.strip(getToken(0).image, "`", "`", "``",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <BRACKET_QUOTED_IDENTIFIER> {
            id = SqlParserUtil.strip(getToken(0).image, "[", "]", "]]",
                quotedCasing);
            pos = getPos().withQuoting(true);
        }
    |
        <UNICODE_QUOTED_IDENTIFIER> {
            span = span();
            String image = getToken(0).image;
            image = image.substring(image.indexOf('"'));
            image = SqlParserUtil.strip(image, DQ, DQ, DQDQ, quotedCasing);
        }
        [
            <UESCAPE> <QUOTED_STRING> {
                String s = SqlParserUtil.parseString(token.image);
                unicodeEscapeChar = SqlParserUtil.checkUnicodeEscapeChar(s);
            }
        ]
        {
            pos = span.end(this).withQuoting(true);
            SqlLiteral lit = SqlLiteral.createCharString(image, "UTF16", pos);
            lit = lit.unescapeUnicode(unicodeEscapeChar);
            id = lit.toValue();
        }
    |
        id = NonReservedKeyWord() {
            pos = getPos();
        }
    )
    {
        if (id.length() > this.identifierMaxLength) {
            throw SqlUtil.newContextException(pos,
                RESOURCE.identifierTooLong(id, this.identifierMaxLength));
        }
        names.add(id);
        if (positions != null) {
            positions.add(pos);
        }
    }
}

/**
 * Parses a simple identifier as a String.
 */
String Identifier() :
{
    final List<String> names = new ArrayList<String>();
}
{
    IdentifierSegment(names, null) {
        return names.get(0);
    }
}

/**
 * Parses a simple identifier as an SqlIdentifier.
 */
SqlIdentifier SimpleIdentifier() :
{
    final List<String> names = new ArrayList<String>();
    final List<SqlParserPos> positions = new ArrayList<SqlParserPos>();
}
{
    IdentifierSegment(names, positions) {
        return new SqlIdentifier(names.get(0), positions.get(0));
    }
}

/**
 * Parses a comma-separated list of simple identifiers.
 */
void SimpleIdentifierCommaList(List<SqlNode> list) :
{
    SqlIdentifier id;
}
{
    id = SimpleIdentifier() {list.add(id);}
    (
        <COMMA> id = SimpleIdentifier() {
            list.add(id);
        }
    )*
}

/**
  * List of simple identifiers in parentheses. The position extends from the
  * open parenthesis to the close parenthesis.
  */
SqlNodeList ParenthesizedSimpleIdentifierList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    SimpleIdentifierCommaList(list)
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

<#if parser.includeCompoundIdentifier>
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
<#if parser.allowColonSeparatorInCompoundIdentifier>
    [
        LOOKAHEAD(2)
        <COLON>
        IdentifierSegment(nameList, posList)
    ]
</#if>
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
 * Parses a comma-separated list of compound identifiers.
 */
void CompoundIdentifierTypeCommaList(List<SqlNode> list, List<SqlNode> extendList) :
{
}
{
    CompoundIdentifierType(list, extendList)
    (<COMMA> CompoundIdentifierType(list, extendList))*
}

/**
 * List of compound identifiers in parentheses. The position extends from the
 * open parenthesis to the close parenthesis.
 */
Pair<SqlNodeList, SqlNodeList> ParenthesizedCompoundIdentifierList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
    final List<SqlNode> extendList = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    CompoundIdentifierTypeCommaList(list, extendList)
    <RPAREN> {
        return Pair.of(new SqlNodeList(list, s.end(this)), new SqlNodeList(extendList, s.end(this)));
    }
}
<#else>
  <#include "/@includes/compoundIdentifier.ftl" />
</#if>

/**
 * Parses a NEW UDT(...) expression.
 */
SqlNode NewSpecification() :
{
    final Span s;
    final SqlNode routineCall;
}
{
    <NEW> { s = span(); }
    routineCall =
        NamedRoutineCall(SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR,
            ExprContext.ACCEPT_SUB_QUERY) {
        return SqlStdOperatorTable.NEW.createCall(s.end(routineCall), routineCall);
    }
}

//TODO: real parse errors.
int UnsignedIntLiteral() :
{
    Token t;
}
{
    t = <UNSIGNED_INTEGER_LITERAL>
    {
        try {
            return Integer.parseInt(t.image);
        } catch (NumberFormatException ex) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.invalidLiteral(t.image, Integer.class.getCanonicalName()));
        }
    }
}

int IntLiteral() :
{
    Token t;
}
{
    (
        t = <UNSIGNED_INTEGER_LITERAL>
    |
        <PLUS> t = <UNSIGNED_INTEGER_LITERAL>
    )
    {
        try {
            return Integer.parseInt(t.image);
        } catch (NumberFormatException ex) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.invalidLiteral(t.image, Integer.class.getCanonicalName()));
        }
    }
|
    <MINUS> t = <UNSIGNED_INTEGER_LITERAL> {
        try {
            return -Integer.parseInt(t.image);
        } catch (NumberFormatException ex) {
            throw SqlUtil.newContextException(getPos(),
                RESOURCE.invalidLiteral(t.image, Integer.class.getCanonicalName()));
        }
    }
}

// Type name with optional scale and precision.
SqlDataTypeSpec DataType() :
{
    SqlTypeNameSpec typeName;
    final Span s;
}
{
    typeName = TypeName() {
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
    |
        typeName = CompoundIdentifier() {
            typeNameSpec = new SqlUserDefinedTypeNameSpec(typeName, s.end(this));
        }
    )
    {
        return typeNameSpec;
    }
}

// Types used for JDBC and ODBC scalar conversion function
SqlTypeNameSpec SqlTypeName(Span s) :
{
    final SqlTypeNameSpec sqlTypeNameSpec;
}
{
    (
        sqlTypeNameSpec = SqlTypeName1(s)
    |
        sqlTypeNameSpec = SqlTypeName2(s)
    |
        sqlTypeNameSpec = SqlTypeName3(s)
    |
        sqlTypeNameSpec = CharacterTypeName(s)
    |
        sqlTypeNameSpec = DateTimeTypeName()
    )
    {
        return sqlTypeNameSpec;
    }
}

// Parse sql type name that don't allow any extra specifications except the type name.
// For extra specification, we mean precision, scale, charSet, etc.
SqlTypeNameSpec SqlTypeName1(Span s) :
{
    final SqlTypeName sqlTypeName;
}
{
    (
        <GEOMETRY> {
            if (!this.conformance.allowGeometry()) {
                throw SqlUtil.newContextException(getPos(), RESOURCE.geometryDisabled());
            }
            sqlTypeName = SqlTypeName.GEOMETRY;
        }
    |
        <BOOLEAN> { sqlTypeName = SqlTypeName.BOOLEAN; }
    |
        ( <INTEGER> | <INT> ) { sqlTypeName = SqlTypeName.INTEGER; }
    |
        <TINYINT> { sqlTypeName = SqlTypeName.TINYINT; }
    |
        <SMALLINT> { sqlTypeName = SqlTypeName.SMALLINT; }
    |
        <BIGINT> { sqlTypeName = SqlTypeName.BIGINT; }
    |
        <REAL> { sqlTypeName = SqlTypeName.REAL; }
    |
        <DOUBLE> { s.add(this); }
        [ <PRECISION> ] { sqlTypeName = SqlTypeName.DOUBLE; }
    |
        <FLOAT> { sqlTypeName = SqlTypeName.FLOAT; }
    )
    {
        return new SqlBasicTypeNameSpec(sqlTypeName, s.end(this));
    }
}

// Parse sql type name that allows precision specification.
SqlTypeNameSpec SqlTypeName2(Span s) :
{
    final SqlTypeName sqlTypeName;
    int precision = -1;
}
{
    (
        <BINARY> { s.add(this); }
        (
            <VARYING> { sqlTypeName = SqlTypeName.VARBINARY; }
        |
            { sqlTypeName = SqlTypeName.BINARY; }
        )
    |
        <VARBINARY> { sqlTypeName = SqlTypeName.VARBINARY; }
    )
    precision = PrecisionOpt()
    {
        return new SqlBasicTypeNameSpec(sqlTypeName, precision, s.end(this));
    }
}

// Parse sql type name that allows precision and scale specifications.
SqlTypeNameSpec SqlTypeName3(Span s) :
{
    final SqlTypeName sqlTypeName;
    int precision = -1;
    int scale = -1;
}
{
    (
        (<DECIMAL> | <DEC> | <NUMERIC>) { sqlTypeName = SqlTypeName.DECIMAL; }
    |
        <ANY> { sqlTypeName = SqlTypeName.ANY; }
    )
    [
        <LPAREN>
        precision = UnsignedIntLiteral()
        [
            <COMMA>
            scale = UnsignedIntLiteral()
        ]
        <RPAREN>
    ]
    {
        return new SqlBasicTypeNameSpec(sqlTypeName, precision, scale, s.end(this));
    }
}

// Types used for for JDBC and ODBC scalar conversion function
SqlJdbcDataTypeName JdbcOdbcDataTypeName() :
{
}
{
    (<SQL_CHAR> | <CHAR>) { return SqlJdbcDataTypeName.SQL_CHAR; }
|   (<SQL_VARCHAR> | <VARCHAR>) { return SqlJdbcDataTypeName.SQL_VARCHAR; }
|   (<SQL_DATE> | <DATE>) { return SqlJdbcDataTypeName.SQL_DATE; }
|   (<SQL_TIME> | <TIME>) { return SqlJdbcDataTypeName.SQL_TIME; }
|   (<SQL_TIMESTAMP> | <TIMESTAMP>) { return SqlJdbcDataTypeName.SQL_TIMESTAMP; }
|   (<SQL_DECIMAL> | <DECIMAL>) { return SqlJdbcDataTypeName.SQL_DECIMAL; }
|   (<SQL_NUMERIC> | <NUMERIC>) { return SqlJdbcDataTypeName.SQL_NUMERIC; }
|   (<SQL_BOOLEAN> | <BOOLEAN>) { return SqlJdbcDataTypeName.SQL_BOOLEAN; }
|   (<SQL_INTEGER> | <INTEGER>) { return SqlJdbcDataTypeName.SQL_INTEGER; }
|   (<SQL_BINARY> | <BINARY>) { return SqlJdbcDataTypeName.SQL_BINARY; }
|   (<SQL_VARBINARY> | <VARBINARY>) { return SqlJdbcDataTypeName.SQL_VARBINARY; }
|   (<SQL_TINYINT> | <TINYINT>) { return SqlJdbcDataTypeName.SQL_TINYINT; }
|   (<SQL_SMALLINT> | <SMALLINT>) { return SqlJdbcDataTypeName.SQL_SMALLINT; }
|   (<SQL_BIGINT> | <BIGINT>) { return SqlJdbcDataTypeName.SQL_BIGINT; }
|   (<SQL_REAL>| <REAL>) { return SqlJdbcDataTypeName.SQL_REAL; }
|   (<SQL_DOUBLE> | <DOUBLE>) { return SqlJdbcDataTypeName.SQL_DOUBLE; }
|   (<SQL_FLOAT> | <FLOAT>) { return SqlJdbcDataTypeName.SQL_FLOAT; }
|   <SQL_INTERVAL_YEAR> { return SqlJdbcDataTypeName.SQL_INTERVAL_YEAR; }
|   <SQL_INTERVAL_YEAR_TO_MONTH> { return SqlJdbcDataTypeName.SQL_INTERVAL_YEAR_TO_MONTH; }
|   <SQL_INTERVAL_MONTH> { return SqlJdbcDataTypeName.SQL_INTERVAL_MONTH; }
|   <SQL_INTERVAL_DAY> { return SqlJdbcDataTypeName.SQL_INTERVAL_DAY; }
|   <SQL_INTERVAL_DAY_TO_HOUR> { return SqlJdbcDataTypeName.SQL_INTERVAL_DAY_TO_HOUR; }
|   <SQL_INTERVAL_DAY_TO_MINUTE> { return SqlJdbcDataTypeName.SQL_INTERVAL_DAY_TO_MINUTE; }
|   <SQL_INTERVAL_DAY_TO_SECOND> { return SqlJdbcDataTypeName.SQL_INTERVAL_DAY_TO_SECOND; }
|   <SQL_INTERVAL_HOUR> { return SqlJdbcDataTypeName.SQL_INTERVAL_HOUR; }
|   <SQL_INTERVAL_HOUR_TO_MINUTE> { return SqlJdbcDataTypeName.SQL_INTERVAL_HOUR_TO_MINUTE; }
|   <SQL_INTERVAL_HOUR_TO_SECOND> { return SqlJdbcDataTypeName.SQL_INTERVAL_HOUR_TO_SECOND; }
|   <SQL_INTERVAL_MINUTE> { return SqlJdbcDataTypeName.SQL_INTERVAL_MINUTE; }
|   <SQL_INTERVAL_MINUTE_TO_SECOND> { return SqlJdbcDataTypeName.SQL_INTERVAL_MINUTE_TO_SECOND; }
|   <SQL_INTERVAL_SECOND> { return SqlJdbcDataTypeName.SQL_INTERVAL_SECOND; }
}

SqlLiteral JdbcOdbcDataType() :
{
    SqlJdbcDataTypeName typeName;
}
{
    typeName = JdbcOdbcDataTypeName() {
        return typeName.symbol(getPos());
    }
}

/**
* Parse a collection type name, the input element type name may
* also be a collection type.
*/
SqlTypeNameSpec CollectionsTypeName(SqlTypeNameSpec elementTypeName) :
{
    final SqlTypeName collectionTypeName;
}
{
    (
        <MULTISET> { collectionTypeName = SqlTypeName.MULTISET; }
    |
        <ARRAY> { collectionTypeName = SqlTypeName.ARRAY; }
    )
    {
        return new SqlCollectionTypeNameSpec(elementTypeName,
                collectionTypeName, getPos());
    }
}

/**
* Parse a nullable option, default is true.
*/
boolean NullableOptDefaultTrue() :
{
}
{
    <NULL> { return true; }
|
    <NOT> <NULL> { return false; }
|
    { return true; }
}

/**
* Parse a nullable option, default is false.
*/
boolean NullableOptDefaultFalse() :
{
}
{
    <NULL> { return true; }
|
    <NOT> <NULL> { return false; }
|
    { return false; }
}

/**
* Parse a "name1 type1 [NULL | NOT NULL], name2 type2 [NULL | NOT NULL] ..." list,
* the field type default is not nullable.
*/
void FieldNameTypeCommaList(
        List<SqlIdentifier> fieldNames,
        List<SqlDataTypeSpec> fieldTypes) :
{
    SqlIdentifier fName;
    SqlDataTypeSpec fType;
    boolean nullable;
}
{
    fName = SimpleIdentifier()
    fType = DataType()
    nullable = NullableOptDefaultFalse()
    {
        fieldNames.add(fName);
        fieldTypes.add(fType.withNullable(nullable));
    }
    (
        <COMMA>
        fName = SimpleIdentifier()
        fType = DataType()
        nullable = NullableOptDefaultFalse()
        {
            fieldNames.add(fName);
            fieldTypes.add(fType.withNullable(nullable));
        }
    )*
}

/**
* Parse Row type with format: Row(name1 type1, name2 type2).
* Every field type can have suffix of `NULL` or `NOT NULL` to indicate if this type is nullable.
* i.e. Row(f0 int not null, f1 varchar null).
*/
SqlTypeNameSpec RowTypeName() :
{
    List<SqlIdentifier> fieldNames = new ArrayList<SqlIdentifier>();
    List<SqlDataTypeSpec> fieldTypes = new ArrayList<SqlDataTypeSpec>();
}
{
    <ROW>
    <LPAREN> FieldNameTypeCommaList(fieldNames, fieldTypes) <RPAREN>
    {
        return new SqlRowTypeNameSpec(getPos(), fieldNames, fieldTypes);
    }
}

/**
* Parse character types: char, varchar.
*/
SqlTypeNameSpec CharacterTypeName(Span s) :
{
    int precision = -1;
    final SqlTypeName sqlTypeName;
    String charSetName = null;
}
{
    (
        (<CHARACTER> | <CHAR>) { s.add(this); }
        (
            <VARYING> { sqlTypeName = SqlTypeName.VARCHAR; }
        |
            { sqlTypeName = SqlTypeName.CHAR; }
        )
    |
        <VARCHAR> { sqlTypeName = SqlTypeName.VARCHAR; }
    )
    precision = PrecisionOpt()
    [
        <CHARACTER> <SET>
        charSetName = Identifier()
    ]
    {
        return new SqlBasicTypeNameSpec(sqlTypeName, precision, charSetName, s.end(this));
    }
}

/**
* Parse datetime types: date, time, timestamp.
*/
SqlTypeNameSpec DateTimeTypeName() :
{
    int precision = -1;
    SqlTypeName typeName;
    boolean withLocalTimeZone = false;
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
    withLocalTimeZone = TimeZoneOpt()
    {
        if (withLocalTimeZone) {
            typeName = SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
        } else {
            typeName = SqlTypeName.TIME;
        }
        return new SqlBasicTypeNameSpec(typeName, precision, getPos());
    }
|
    <TIMESTAMP>
    precision = PrecisionOpt()
    withLocalTimeZone = TimeZoneOpt()
    {
        if (withLocalTimeZone) {
            typeName = SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else {
            typeName = SqlTypeName.TIMESTAMP;
        }
        return new SqlBasicTypeNameSpec(typeName, precision, getPos());
    }
}

// Parse an optional data type precision, default is -1.
int PrecisionOpt() :
{
    int precision = -1;
}
{
    LOOKAHEAD(2)
    <LPAREN>
    precision = UnsignedIntLiteral()
    <RPAREN>
    { return precision; }
|
    { return -1; }
}

/**
* Parse a time zone suffix for DateTime types. According to SQL-2011,
* "with time zone" and "without time zone" belong to standard SQL but we
* only implement the "without time zone".
*
* <p>We also support "with local time zone".
*
* @return true if this is "with local time zone".
*/
boolean TimeZoneOpt() :
{
}
{
    LOOKAHEAD(3)
    <WITHOUT> <TIME> <ZONE> { return false; }
|
    <WITH> <LOCAL> <TIME> <ZONE> { return true; }
|
    { return false; }
}

/**
 * Parses a CURSOR(query) expression.  The parser allows these
 * anywhere, but the validator restricts them to appear only as
 * arguments to table functions.
 */
SqlNode CursorExpression(ExprContext exprContext) :
{
    final SqlNode e;
    final Span s;
}
{
    <CURSOR> {
        s = span();
        if (exprContext != ExprContext.ACCEPT_ALL
                && exprContext != ExprContext.ACCEPT_CURSOR) {
            throw SqlUtil.newContextException(s.end(this),
                RESOURCE.illegalCursorExpression());
        }
    }
    e = Expression(ExprContext.ACCEPT_QUERY) {
        return SqlStdOperatorTable.CURSOR.createCall(s.end(e), e);
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
<#if parser.allowTranslateUsingCharSetWithError>
    boolean isWithError = false;
    boolean allowTranslateUsingCharSet = false;
</#if>
}
{
    //~ FUNCTIONS WITH SPECIAL SYNTAX ---------------------------------------
    (
        <CAST> { s = span(); }
        <LPAREN> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args = startList(e); }
        <AS>
<#if parser.includeCastDataAttributes>
        (
            (
                dt = DataType() { args.add(dt); }
            |
                <INTERVAL> e = IntervalQualifier() { args.add(e); }
            )
            ( e = AlternativeTypeConversionAttribute() { args.add(e); } )*
        |
            ( e = AlternativeTypeConversionAttribute() { args.add(e); } )+
        )
<#else>
        (
            dt = DataType() { args.add(dt); }
        |
            <INTERVAL> e = IntervalQualifier() { args.add(e); }
        )
</#if>
        <RPAREN> {
            return SqlStdOperatorTable.CAST.createCall(s.end(this), args);
        }
    |
        <EXTRACT> {
            s = span();
        }
        <LPAREN>
        (
            <NANOSECOND> { unit = TimeUnit.NANOSECOND; }
        |   <MICROSECOND> { unit = TimeUnit.MICROSECOND; }
        |   unit = TimeUnit()
        )
        { args = startList(new SqlIntervalQualifier(unit, null, getPos())); }
        <FROM>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
        <RPAREN> {
            return SqlStdOperatorTable.EXTRACT.createCall(s.end(this), args);
        }
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
        <TRANSLATE> { s = span(); }
        <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args = startList(e);
        }
        (
            <USING> name = SimpleIdentifier() {
<#if parser.allowTranslateUsingCharSetWithError>
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
<#else>
                args.add(name);
</#if>
            }
<#if parser.allowTranslateUsingCharSetWithError>
            [
                <WITH> <ERROR> {
                    isWithError = true;
                }
            ]
</#if>
            <RPAREN> {
<#if parser.allowTranslateUsingCharSetWithError>
                if (allowTranslateUsingCharSet) {
                    return new SqlTranslateUsingCharacterSet(s.end(this), args, isWithError);
                }
</#if>
                return SqlStdOperatorTable.TRANSLATE.createCall(s.end(this),
                    args);
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
<#if parser.includeSubstrKeyword>
        |
            <SUBSTR>
</#if>
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
<#list parser.builtinFunctionCallMethods as method>
        node = ${method} { return node; }
    |
</#list>
<#if parser.includeRankWithSortingExpressions>
        LOOKAHEAD(<RANK>, { getToken(3).kind != RPAREN })
        node = RankFunctionCallWithParams() { return node; }
    |
</#if>
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
    )
}

SqlJsonEncoding JsonRepresentation() :
{

}
{
    <JSON>
    [
        // Encoding is currently ignored.
        LOOKAHEAD(2) <ENCODING>
        (
            <UTF8> { return SqlJsonEncoding.UTF8; }
            |
            <UTF16> { return SqlJsonEncoding.UTF16; }
            |
            <UTF32> { return SqlJsonEncoding.UTF32; }
        )
    ]
    {
        return SqlJsonEncoding.UTF8;
    }
}

void JsonInputClause() :
{

}
{
    <FORMAT> JsonRepresentation()
}

SqlDataTypeSpec JsonReturningClause() :
{
    SqlDataTypeSpec dt;
}
{
    <RETURNING> dt = DataType() { return dt; }
}

SqlDataTypeSpec JsonOutputClause() :
{
    SqlDataTypeSpec dataType;
}
{
    dataType = JsonReturningClause()
    [
        <FORMAT> JsonRepresentation()
    ]
    {
        return dataType;
    }
}

SqlNode JsonPathSpec() :
{
    SqlNode e;
}
{
    e = StringLiteral() {
        return e;
    }
}

List<SqlNode> JsonApiCommonSyntax() :
{
    SqlNode e;
    List<SqlNode> args = new ArrayList<SqlNode>();
}
{
    e = Expression(ExprContext.ACCEPT_NON_QUERY) {
        args.add(e);
    }
    (
        <COMMA>
        e = Expression(ExprContext.ACCEPT_NON_QUERY) {
            args.add(e);
        }
    )
    [
        // We currently don't support JSON passing clause, leave the java code blocks no-op
        <PASSING> e = Expression(ExprContext.ACCEPT_NON_QUERY) {
            // no-op
        }
        <AS> e = SimpleIdentifier() {
            // no-op
        }
        (
            <COMMA>
            e = Expression(ExprContext.ACCEPT_NON_QUERY) {
                // no-op
            }
            <AS> e = SimpleIdentifier() {
                // no-op
            }
        )*
    ]
    {
        return args;
    }
}

SqlJsonExistsErrorBehavior JsonExistsErrorBehavior() :
{

}
{
    <TRUE> { return SqlJsonExistsErrorBehavior.TRUE; }
    |
    <FALSE> { return SqlJsonExistsErrorBehavior.FALSE; }
    |
    <UNKNOWN> { return SqlJsonExistsErrorBehavior.UNKNOWN; }
    |
    <ERROR> { return SqlJsonExistsErrorBehavior.ERROR; }
}

SqlCall JsonExistsFunctionCall() :
{
    List<SqlNode> args = new ArrayList<SqlNode>();
    List<SqlNode> commonSyntax;
    final Span span;
    SqlJsonExistsErrorBehavior errorBehavior;
}
{
    <JSON_EXISTS> { span = span(); }
    <LPAREN> commonSyntax = JsonApiCommonSyntax() {
        args.addAll(commonSyntax);
    }
    [
        errorBehavior = JsonExistsErrorBehavior() { args.add(errorBehavior.symbol(getPos())); }
        <ON> <ERROR>
    ]
    <RPAREN> {
        return SqlStdOperatorTable.JSON_EXISTS.createCall(span.end(this), args);
    }
}

List<SqlNode> JsonValueEmptyOrErrorBehavior() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    final SqlNode e;
}
{
    (
        <ERROR> {
            list.add(SqlJsonValueEmptyOrErrorBehavior.ERROR.symbol(getPos()));
        }
    |
        <NULL> {
            list.add(SqlJsonValueEmptyOrErrorBehavior.NULL.symbol(getPos()));
        }
    |
        <DEFAULT_> e = Expression(ExprContext.ACCEPT_NON_QUERY) {
            list.add(SqlJsonValueEmptyOrErrorBehavior.DEFAULT.symbol(getPos()));
            list.add(e);
        }
    )
    <ON>
    (
        <EMPTY> {
            list.add(SqlJsonEmptyOrError.EMPTY.symbol(getPos()));
        }
    |
        <ERROR> {
            list.add(SqlJsonEmptyOrError.ERROR.symbol(getPos()));
        }
    )
    { return list; }
}

SqlCall JsonValueFunctionCall() :
{
    final List<SqlNode> args = new ArrayList<SqlNode>(7);
    SqlNode e;
    List<SqlNode> commonSyntax;
    final Span span;
    List<SqlNode> behavior;
}
{
    <JSON_VALUE> { span = span(); }
    <LPAREN> commonSyntax = JsonApiCommonSyntax() {
        args.addAll(commonSyntax);
    }
    [
        e = JsonReturningClause() {
            args.add(SqlJsonValueReturning.RETURNING.symbol(getPos()));
            args.add(e);
        }
    ]
    (
        behavior = JsonValueEmptyOrErrorBehavior() {
            args.addAll(behavior);
        }
    )*
    <RPAREN> {
        return SqlStdOperatorTable.JSON_VALUE.createCall(span.end(this), args);
    }
}

List<SqlNode> JsonQueryEmptyOrErrorBehavior() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode e;
}
{
    (
        <ERROR> {
            list.add(SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.ERROR, getPos()));
        }
    |
        <NULL> {
            list.add(SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.NULL, getPos()));
        }
    |
        LOOKAHEAD(2)
        <EMPTY> <ARRAY> {
            list.add(SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.EMPTY_ARRAY, getPos()));
        }
    |
        <EMPTY> <OBJECT> {
            list.add(SqlLiteral.createSymbol(SqlJsonQueryEmptyOrErrorBehavior.EMPTY_OBJECT, getPos()));
        }
    )
    <ON>
    (
        <EMPTY> {
            list.add(SqlLiteral.createSymbol(SqlJsonEmptyOrError.EMPTY, getPos()));
        }
    |
        <ERROR> {
            list.add(SqlLiteral.createSymbol(SqlJsonEmptyOrError.ERROR, getPos()));
        }
    )
    { return list; }
}

SqlNode JsonQueryWrapperBehavior() :
{
    SqlNode e;
}
{
    <WITHOUT> [<ARRAY>] {
        return SqlLiteral.createSymbol(SqlJsonQueryWrapperBehavior.WITHOUT_ARRAY, getPos());
    }
|
    LOOKAHEAD(2)
    <WITH> <CONDITIONAL> [<ARRAY>] {
        return SqlLiteral.createSymbol(SqlJsonQueryWrapperBehavior.WITH_CONDITIONAL_ARRAY, getPos());
    }
|
    <WITH> [<UNCONDITIONAL>] [<ARRAY>] {
        return SqlLiteral.createSymbol(SqlJsonQueryWrapperBehavior.WITH_UNCONDITIONAL_ARRAY, getPos());
    }
}

SqlCall JsonQueryFunctionCall() :
{
    final SqlNode[] args = new SqlNode[5];
    SqlNode e;
    List<SqlNode> commonSyntax;
    final Span span;
    List<SqlNode> behavior;
}
{
    <JSON_QUERY> { span = span(); }
    <LPAREN> commonSyntax = JsonApiCommonSyntax() {
        args[0] = commonSyntax.get(0);
        args[1] = commonSyntax.get(1);
    }
    [
        e = JsonQueryWrapperBehavior() <WRAPPER> {
            args[2] = e;
        }
    ]
    (
        behavior = JsonQueryEmptyOrErrorBehavior() {
            final SqlJsonEmptyOrError symbol =
                ((SqlLiteral) behavior.get(1)).getValueAs(SqlJsonEmptyOrError.class);
            switch (symbol) {
            case EMPTY:
                args[3] = behavior.get(0);
                break;
            case ERROR:
                args[4] = behavior.get(0);
                break;
            }
        }
    )*
    <RPAREN> {
        return SqlStdOperatorTable.JSON_QUERY.createCall(span.end(this), args);
    }
}

SqlNode JsonName() :
{
    final SqlNode e;
}
{
    (
        e = SimpleIdentifier()
    |
        e = StringLiteral()
    |
        e = NumericLiteral()
    )
    {
        return e;
    }
}

List<SqlNode> JsonNameAndValue() :
{
    final List<SqlNode> list = new ArrayList<SqlNode>();
    SqlNode e;
    boolean kvMode = false;
}
{
    [
        LOOKAHEAD(2, <KEY> JsonName())
        <KEY> { kvMode = true; }
    ]
    e = JsonName() {
        list.add(e);
    }
    (
        <VALUE>
    |
        <COLON> {
            if (kvMode) {
                throw SqlUtil.newContextException(getPos(), RESOURCE.illegalColon());
            }
        }
    )
    e = Expression(ExprContext.ACCEPT_NON_QUERY) {
        list.add(e);
    }
    {
        return list;
    }
}

SqlNode JsonConstructorNullClause() :
{
}
{
    <NULL> <ON> <NULL> {
        return SqlLiteral.createSymbol(SqlJsonConstructorNullClause.NULL_ON_NULL, getPos());
    }
|
    <ABSENT> <ON> <NULL> {
        return SqlLiteral.createSymbol(SqlJsonConstructorNullClause.ABSENT_ON_NULL, getPos());
    }
}

SqlCall JsonObjectFunctionCall() :
{
    final List<SqlNode> nvArgs = new ArrayList<SqlNode>();
    final SqlNode[] otherArgs = new SqlNode[1];
    SqlNode e;
    List<SqlNode> list;
    final Span span;
}
{
    <JSON_OBJECT> { span = span(); }
    <LPAREN> [
        LOOKAHEAD(2)
        list = JsonNameAndValue() {
            nvArgs.addAll(list);
        }
        (
            <COMMA>
            list = JsonNameAndValue() {
                nvArgs.addAll(list);
            }
        )*
    ]
    [
        e = JsonConstructorNullClause() {
            otherArgs[0] = e;
        }
    ]
    <RPAREN> {
        final List<SqlNode> args = new ArrayList();
        args.addAll(Arrays.asList(otherArgs));
        args.addAll(nvArgs);
        return SqlStdOperatorTable.JSON_OBJECT.createCall(span.end(this), args);
    }
}

SqlCall JsonObjectAggFunctionCall() :
{
    final SqlNode[] args = new SqlNode[2];
    List<SqlNode> list;
    final Span span;
    SqlJsonConstructorNullClause nullClause =
        SqlJsonConstructorNullClause.NULL_ON_NULL;
    final SqlNode e;
}
{
    <JSON_OBJECTAGG> { span = span(); }
    <LPAREN> list = JsonNameAndValue() {
        args[0] = list.get(0);
        args[1] = list.get(1);
    }
    [
        e = JsonConstructorNullClause() {
            nullClause = (SqlJsonConstructorNullClause) ((SqlLiteral) e).getValue();
        }
    ]
    <RPAREN> {
        return SqlStdOperatorTable.JSON_OBJECTAGG.with(nullClause)
            .createCall(span.end(this), args);
    }
}

SqlCall JsonArrayFunctionCall() :
{
    final List<SqlNode> elements = new ArrayList<SqlNode>();
    final SqlNode[] otherArgs = new SqlNode[1];
    SqlNode e;
    final Span span;
}
{
    <JSON_ARRAY> { span = span(); }
    <LPAREN> [
        LOOKAHEAD(2)
        e = Expression(ExprContext.ACCEPT_NON_QUERY) {
            elements.add(e);
        }
        (
            <COMMA>
            e = Expression(ExprContext.ACCEPT_NON_QUERY) {
                elements.add(e);
            }
        )*
    ]
    [
        e = JsonConstructorNullClause() {
            otherArgs[0] = e;
        }
    ]
    <RPAREN> {
        final List<SqlNode> args = new ArrayList();
        args.addAll(Arrays.asList(otherArgs));
        args.addAll(elements);
        return SqlStdOperatorTable.JSON_ARRAY.createCall(span.end(this), args);
    }
}

SqlNodeList JsonArrayAggOrderByClause() :
{
    final SqlNodeList orderList;
}
{
    (
        orderList = OrderBy(true)
    |
        { orderList = null; }
    )
    {
        return orderList;
    }
}

SqlCall JsonArrayAggFunctionCall() :
{
    final SqlNode valueExpr;
    SqlNodeList orderList = null;
    List<SqlNode> list;
    final Span span;
    SqlJsonConstructorNullClause nullClause =
        SqlJsonConstructorNullClause.ABSENT_ON_NULL;
    SqlNode e = null;
    final SqlNode aggCall;
}
{
    <JSON_ARRAYAGG> { span = span(); }
    <LPAREN> e = Expression(ExprContext.ACCEPT_NON_QUERY) {
        valueExpr = e;
    }
    orderList = JsonArrayAggOrderByClause()
    [
        e = JsonConstructorNullClause() {
            nullClause = (SqlJsonConstructorNullClause) ((SqlLiteral) e).getValue();
        }
    ]
    <RPAREN>
    {
        aggCall = SqlStdOperatorTable.JSON_ARRAYAGG.with(nullClause)
            .createCall(span.end(this), valueExpr, orderList);
    }
    [
        e = withinGroup(aggCall) {
            if (orderList != null) {
                throw SqlUtil.newContextException(span.pos().plus(e.getParserPosition()),
                    RESOURCE.ambiguousSortOrderInJsonArrayAggFunc());
            }
            return (SqlCall) e;
        }
    ]
    {
        if (orderList == null) {
            return SqlStdOperatorTable.JSON_ARRAYAGG.with(nullClause)
                .createCall(span.end(this), valueExpr);
        }
        return SqlStdOperatorTable.JSON_ARRAYAGG.with(nullClause)
            .createCall(span.end(this), valueExpr, orderList);
    }
}

/**
 * Parses a call to TIMESTAMPADD.
 */
SqlCall TimestampAddFunctionCall() :
{
    List<SqlNode> args;
    SqlNode e;
    final Span s;
    TimeUnit interval;
    SqlNode node;
}
{
    <TIMESTAMPADD> { s = span(); }
    <LPAREN>
    interval = TimestampInterval() {
        args = startList(SqlLiteral.createSymbol(interval, getPos()));
    }
    <COMMA>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
    <COMMA>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
    <RPAREN> {
        return SqlStdOperatorTable.TIMESTAMP_ADD.createCall(
            s.end(this), args);
    }
}

/**
 * Parses a call to TIMESTAMPDIFF.
 */
SqlCall TimestampDiffFunctionCall() :
{
    List<SqlNode> args;
    SqlNode e;
    final Span s;
    TimeUnit interval;
    SqlNode node;
}
{
    <TIMESTAMPDIFF> { s = span(); }
    <LPAREN>
    interval = TimestampInterval() {
        args = startList(SqlLiteral.createSymbol(interval, getPos()));
    }
    <COMMA>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
    <COMMA>
    e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args.add(e); }
    <RPAREN> {
        return SqlStdOperatorTable.TIMESTAMP_DIFF.createCall(
            s.end(this), args);
    }
}

/**
 * Parses a call to a grouping function inside the GROUP BY clause,
 * for example {@code TUMBLE(rowtime, INTERVAL '1' MINUTE)}.
 */
SqlCall GroupByWindowingCall():
{
    final Span s;
    final List<SqlNode> args;
    final SqlOperator op;
}
{
    (
        <TUMBLE>
        {
            s = span();
            op = SqlStdOperatorTable.TUMBLE_OLD;
        }
    |
        <HOP>
        {
            s = span();
            op = SqlStdOperatorTable.HOP_OLD;
        }
    |
        <SESSION>
        {
            s = span();
            op = SqlStdOperatorTable.SESSION_OLD;
        }
    )
    args = UnquantifiedFunctionParameterList(ExprContext.ACCEPT_SUB_QUERY) {
        return op.createCall(s.end(this), args);
    }
}

SqlCall MatchRecognizeFunctionCall() :
{
    final SqlCall func;
    final Span s;
}
{
    (
        <CLASSIFIER> { s = span(); } <LPAREN> <RPAREN> {
            func = SqlStdOperatorTable.CLASSIFIER.createCall(s.end(this));
        }
    |
        <MATCH_NUMBER> { s = span(); } <LPAREN> <RPAREN> {
            func = SqlStdOperatorTable.MATCH_NUMBER.createCall(s.end(this));
        }
    |
        LOOKAHEAD(3)
        func = MatchRecognizeNavigationLogical()
    |
        LOOKAHEAD(2)
        func = MatchRecognizeNavigationPhysical()
    |
        func = MatchRecognizeCallWithModifier()
    )
    { return func; }
}

SqlCall MatchRecognizeCallWithModifier() :
{
    final Span s;
    final SqlOperator runningOp;
    final SqlNode func;
}
{
    (
        <RUNNING> { runningOp = SqlStdOperatorTable.RUNNING; }
    |
        <FINAL> { runningOp = SqlStdOperatorTable.FINAL; }
    )
    { s = span(); }
    func = NamedFunctionCall() {
        return runningOp.createCall(s.end(func), func);
    }
}

SqlCall MatchRecognizeNavigationLogical() :
{
    final Span s = Span.of();
    SqlCall func;
    final SqlOperator funcOp;
    final SqlOperator runningOp;
    SqlNode arg0;
    SqlNode arg1 = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
}
{
    (
        <RUNNING> { runningOp = SqlStdOperatorTable.RUNNING; s.add(this); }
    |
        <FINAL> { runningOp = SqlStdOperatorTable.FINAL; s.add(this); }
    |
        { runningOp = null; }
    )
    (
        <FIRST> { funcOp = SqlStdOperatorTable.FIRST; }
    |
        <LAST> { funcOp = SqlStdOperatorTable.LAST; }
    )
    { s.add(this); }
    <LPAREN>
    arg0 = Expression(ExprContext.ACCEPT_SUB_QUERY)
    [ <COMMA> arg1 = NumericLiteral() ]
    <RPAREN> {
        func = funcOp.createCall(s.end(this), arg0, arg1);
        if (runningOp != null) {
            return runningOp.createCall(s.end(this), func);
        } else {
            return func;
        }
    }
}

SqlCall MatchRecognizeNavigationPhysical() :
{
    final Span s;
    SqlCall func;
    SqlOperator funcOp;
    SqlNode arg0;
    SqlNode arg1 = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
}
{
    (
        <PREV> { funcOp = SqlStdOperatorTable.PREV; }
    |
        <NEXT> { funcOp = SqlStdOperatorTable.NEXT; }
    )
    { s = span(); }
    <LPAREN>
    arg0 = Expression(ExprContext.ACCEPT_SUB_QUERY)
    [ <COMMA> arg1 = NumericLiteral() ]
    <RPAREN> {
        return funcOp.createCall(s.end(this), arg0, arg1);
    }
}

SqlCall withinGroup(SqlNode arg) :
{
    final Span withinGroupSpan;
    final SqlNodeList orderList;
}
{

    <WITHIN> { withinGroupSpan = span(); }
    <GROUP>
    <LPAREN>
    orderList = OrderBy(true)
    <RPAREN> {
        return SqlStdOperatorTable.WITHIN_GROUP.createCall(
            withinGroupSpan.end(this), arg, orderList);
    }
}

SqlCall nullTreatment(SqlCall arg) :
{
    final Span span;
}
{
    (
        <IGNORE> { span = span(); } <NULLS> {
            return SqlStdOperatorTable.IGNORE_NULLS.createCall(
                span.end(this), arg);
        }
    |
        <RESPECT> { span = span(); } <NULLS> {
            return SqlStdOperatorTable.RESPECT_NULLS.createCall(
                span.end(this), arg);
        }
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
<#if parser.postNamedFunctionCallMethods?size != 0>
    SqlNode e;
</#if>
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
        LOOKAHEAD(2) call = nullTreatment(call)
    ]
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
    (
<#list parser.postNamedFunctionCallMethods as method>
        e = ${method}(call) { return e; }
    |
</#list>
        { return call; }
    )
}


/*
* Parse Floor/Ceil function parameters
*/
SqlNode StandardFloorCeilOptions(Span s, boolean floorFlag) :
{
    SqlNode e;
    final List<SqlNode> args;
    TimeUnit unit;
    SqlCall function;
    final Span s1;
}
{
    <LPAREN> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        args = startList(e);
    }
    (
        <TO>
        unit = TimeUnit() {
            args.add(new SqlIntervalQualifier(unit, null, getPos()));
        }
    )?
    <RPAREN> {
        SqlOperator op = floorFlag
            ? SqlStdOperatorTable.FLOOR
            : SqlStdOperatorTable.CEIL;
        function =  op.createCall(s.end(this), args);
    }
    (
        <OVER> { s1 = span(); }
        (
            e = SimpleIdentifier()
        |
            e = WindowSpecification()
        )
        {
            return SqlStdOperatorTable.OVER.createCall(s1.end(this), function, e);
        }
    |
        { return function; }
    )
}

/**
 * Parses the name of a JDBC function that is a token but is not reserved.
 */
String NonReservedJdbcFunctionName() :
{
}
{
    (
        <SUBSTRING>
    )
    {
        return unquotedIdentifier();
    }
}

/**
 * Parses the name of a function (either a compound identifier or
 * a reserved word which can be used as a function name).
 */
SqlIdentifier FunctionName() :
{
    SqlIdentifier qualifiedName;
}
{
    (
        qualifiedName = CompoundIdentifier()
    |
        qualifiedName = ReservedFunctionName()
    )
    {
        return qualifiedName;
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
    |   <FIRST_VALUE>
    |   <FLOOR>
    |   <FUSION>
    |   <INTERSECTION>
    |   <GROUPING>
    |   <HOUR>
    |   <LAG>
    |   <LEAD>
    |   <LEFT>
    |   <LAST_VALUE>
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

SqlIdentifier ContextVariable() :
{
}
{
    (
        <CURRENT_CATALOG>
    |   <CURRENT_DATE>
    |   <CURRENT_DEFAULT_TRANSFORM_GROUP>
    |   <CURRENT_PATH>
    |   <CURRENT_ROLE>
    |   <CURRENT_SCHEMA>
    |   <CURRENT_TIME>
    |   <CURRENT_TIMESTAMP>
    |   <CURRENT_USER>
    |   <LOCALTIME>
    |   <LOCALTIMESTAMP>
    |   <SESSION_USER>
    |   <SYSTEM_USER>
    |   <USER>
    )
    {
        return new SqlIdentifier(unquotedIdentifier(), getPos());
    }
}

/**
 * Parses a function call expression with JDBC syntax.
 */
SqlNode JdbcFunctionCall() :
{
    String name;
    SqlIdentifier id;
    SqlNode e;
    SqlLiteral tl;
    SqlNodeList args;
    SqlCall call;
    final Span s, s1;
}
{
    <LBRACE_FN> {
        s = span();
    }
    (
        LOOKAHEAD(1)
        call = TimestampAddFunctionCall() {
            name = call.getOperator().getName();
            args = new SqlNodeList(call.getOperandList(), getPos());
        }
    |
        LOOKAHEAD(3)
        call = TimestampDiffFunctionCall() {
            name = call.getOperator().getName();
            args = new SqlNodeList(call.getOperandList(), getPos());
        }
    |
        <CONVERT> { name = unquotedIdentifier(); }
        <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args = new SqlNodeList(getPos());
            args.add(e);
        }
        <COMMA>
        tl = JdbcOdbcDataType() { args.add(tl); }
        <RPAREN>
    |
        (
            // INSERT is a reserved word, but we need to handle {fn insert}
            // Similarly LEFT, RIGHT, TRUNCATE
            LOOKAHEAD(1)
            ( <INSERT> | <LEFT> | <RIGHT> | <TRUNCATE> ) { name = unquotedIdentifier(); }
        |
            // For cases like {fn power(1,2)} and {fn lower('a')}
            id = ReservedFunctionName() { name = id.getSimple(); }
        |
            // For cases like {fn substring('foo', 1,2)}
            name = NonReservedJdbcFunctionName()
        |
            name = Identifier()
        )
        (
            LOOKAHEAD(2) <LPAREN> <STAR> { s1 = span(); } <RPAREN>
            {
                args = new SqlNodeList(s1.pos());
                args.add(SqlIdentifier.star(s1.pos()));
            }
        |
            LOOKAHEAD(2) <LPAREN> <RPAREN> { args = SqlNodeList.EMPTY; }
        |
            args = ParenthesizedQueryOrCommaList(ExprContext.ACCEPT_SUB_QUERY)
        )
    )
    <RBRACE> {
        return new SqlJdbcFunctionCall(name).createCall(s.end(this),
            args.getList());
    }
}

/**
 * Parses a binary query operator like UNION.
 */
SqlBinaryOperator BinaryQueryOperator() :
{
}
{
    // If both the ALL or DISTINCT keywords are missing, DISTINCT is implicit.
    (
        <UNION>
        (
            <ALL> { return SqlStdOperatorTable.UNION_ALL; }
        |   <DISTINCT> { return SqlStdOperatorTable.UNION; }
        |   { return SqlStdOperatorTable.UNION; }
        )
    |
        <INTERSECT>
        (
            <ALL> { return SqlStdOperatorTable.INTERSECT_ALL; }
        |   <DISTINCT> { return SqlStdOperatorTable.INTERSECT; }
        |   { return SqlStdOperatorTable.INTERSECT; }
        )
    |
        (
            <EXCEPT>
        |
            <SET_MINUS> {
                if (!this.conformance.isMinusAllowed()) {
                    throw SqlUtil.newContextException(getPos(), RESOURCE.minusNotAllowed());
                }
            }
        )
        (
            <ALL> { return SqlStdOperatorTable.EXCEPT_ALL; }
        |   <DISTINCT> { return SqlStdOperatorTable.EXCEPT; }
        |   { return SqlStdOperatorTable.EXCEPT; }
        )
    )
}

/**
 * Parses a binary multiset operator.
 */
SqlBinaryOperator BinaryMultisetOperator() :
{
}
{
    // If both the ALL or DISTINCT keywords are missing, DISTINCT is implicit
    <MULTISET>
    (
        <UNION>
        [
            <ALL>
        |   <DISTINCT> { return SqlStdOperatorTable.MULTISET_UNION_DISTINCT; }
        ]
        { return SqlStdOperatorTable.MULTISET_UNION; }
    |
        <INTERSECT>
        [
            <ALL>
        |   <DISTINCT> { return SqlStdOperatorTable.MULTISET_INTERSECT_DISTINCT; }
        ]
        { return SqlStdOperatorTable.MULTISET_INTERSECT; }
    |
        <EXCEPT>
        [
            <ALL>
        |   <DISTINCT> { return SqlStdOperatorTable.MULTISET_EXCEPT_DISTINCT; }
        ]
        { return SqlStdOperatorTable.MULTISET_EXCEPT; }
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
    // <IN> is handled as a special case
    <EQ> { return SqlStdOperatorTable.EQUALS; }
|   <GT> { return SqlStdOperatorTable.GREATER_THAN; }
|   <LT> { return SqlStdOperatorTable.LESS_THAN; }
|   <LE> { return SqlStdOperatorTable.LESS_THAN_OR_EQUAL; }
|   <GE> { return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL; }
|   <NE> { return SqlStdOperatorTable.NOT_EQUALS; }
|   <NE2> {
        if (!this.conformance.isBangEqualAllowed()) {
            throw SqlUtil.newContextException(getPos(), RESOURCE.bangEqualNotAllowed());
        }
        return SqlStdOperatorTable.NOT_EQUALS;
    }
|   <PLUS> { return SqlStdOperatorTable.PLUS; }
|   <MINUS> { return SqlStdOperatorTable.MINUS; }
|   <STAR> { return SqlStdOperatorTable.MULTIPLY; }
|   <SLASH> { return SqlStdOperatorTable.DIVIDE; }
|   <PERCENT_REMAINDER> {
        if (!this.conformance.isPercentRemainderAllowed()) {
            throw SqlUtil.newContextException(getPos(), RESOURCE.percentRemainderNotAllowed());
        }
        return SqlStdOperatorTable.PERCENT_REMAINDER;
    }
|   <CONCAT> { return SqlStdOperatorTable.CONCAT; }
|   <AND> { return SqlStdOperatorTable.AND; }
|   <OR> { return SqlStdOperatorTable.OR; }
|   LOOKAHEAD(2) <IS> <DISTINCT> <FROM> { return SqlStdOperatorTable.IS_DISTINCT_FROM; }
|   <IS> <NOT> <DISTINCT> <FROM> { return SqlStdOperatorTable.IS_NOT_DISTINCT_FROM; }
|   <MEMBER> <OF> { return SqlStdOperatorTable.MEMBER_OF; }
|   LOOKAHEAD(2) <SUBMULTISET> <OF> { return SqlStdOperatorTable.SUBMULTISET_OF; }
|   <NOT> <SUBMULTISET> <OF> { return SqlStdOperatorTable.NOT_SUBMULTISET_OF; }
|   <CONTAINS> { return SqlStdOperatorTable.CONTAINS; }
|   <OVERLAPS> { return SqlStdOperatorTable.OVERLAPS; }
|   <EQUALS> { return SqlStdOperatorTable.PERIOD_EQUALS; }
|   <PRECEDES> { return SqlStdOperatorTable.PRECEDES; }
|   <SUCCEEDS> { return SqlStdOperatorTable.SUCCEEDS; }
|   LOOKAHEAD(2) <IMMEDIATELY> <PRECEDES> { return SqlStdOperatorTable.IMMEDIATELY_PRECEDES; }
|   <IMMEDIATELY> <SUCCEEDS> { return SqlStdOperatorTable.IMMEDIATELY_SUCCEEDS; }
|   op = BinaryMultisetOperator() { return op; }
}

/**
 * Parses a prefix row operator like NOT.
 */
SqlPrefixOperator PrefixRowOperator() :
{}
{
    <PLUS> { return SqlStdOperatorTable.UNARY_PLUS; }
|   <MINUS> { return SqlStdOperatorTable.UNARY_MINUS; }
|   <NOT> { return SqlStdOperatorTable.NOT; }
|   <EXISTS> { return SqlStdOperatorTable.EXISTS; }
}

/**
 * Parses a postfix row operator like IS NOT NULL.
 */
SqlPostfixOperator PostfixRowOperator() :
{}
{
    <IS>
    (
        <A> <SET> { return SqlStdOperatorTable.IS_A_SET; }
    |
        <NOT>
        (
            <NULL> { return SqlStdOperatorTable.IS_NOT_NULL; }
        |   <TRUE> { return SqlStdOperatorTable.IS_NOT_TRUE; }
        |   <FALSE> { return SqlStdOperatorTable.IS_NOT_FALSE; }
        |   <UNKNOWN> { return SqlStdOperatorTable.IS_NOT_UNKNOWN; }
        |   <A> <SET> { return SqlStdOperatorTable.IS_NOT_A_SET; }
        |   <EMPTY> { return SqlStdOperatorTable.IS_NOT_EMPTY; }
        |   LOOKAHEAD(2) <JSON> <VALUE> { return SqlStdOperatorTable.IS_NOT_JSON_VALUE; }
        |   LOOKAHEAD(2) <JSON> <OBJECT> { return SqlStdOperatorTable.IS_NOT_JSON_OBJECT; }
        |   LOOKAHEAD(2) <JSON> <ARRAY> { return SqlStdOperatorTable.IS_NOT_JSON_ARRAY; }
        |   LOOKAHEAD(2) <JSON> <SCALAR> { return SqlStdOperatorTable.IS_NOT_JSON_SCALAR; }
        |   <JSON> { return SqlStdOperatorTable.IS_NOT_JSON_VALUE; }
        )
    |
        (
            <NULL> { return SqlStdOperatorTable.IS_NULL; }
        |   <TRUE> { return SqlStdOperatorTable.IS_TRUE; }
        |   <FALSE> { return SqlStdOperatorTable.IS_FALSE; }
        |   <UNKNOWN> { return SqlStdOperatorTable.IS_UNKNOWN; }
        |   <EMPTY> { return SqlStdOperatorTable.IS_EMPTY; }
        |   LOOKAHEAD(2) <JSON> <VALUE> { return SqlStdOperatorTable.IS_JSON_VALUE; }
        |   LOOKAHEAD(2) <JSON> <OBJECT> { return SqlStdOperatorTable.IS_JSON_OBJECT; }
        |   LOOKAHEAD(2) <JSON> <ARRAY> { return SqlStdOperatorTable.IS_JSON_ARRAY; }
        |   LOOKAHEAD(2) <JSON> <SCALAR> { return SqlStdOperatorTable.IS_JSON_SCALAR; }
        |   <JSON> { return SqlStdOperatorTable.IS_JSON_VALUE; }
        )
    )
|
    <FORMAT>
    (
        JsonRepresentation() {
            return SqlStdOperatorTable.JSON_VALUE_EXPRESSION;
        }
    )
}


/* KEYWORDS:  anything in this list is a reserved word unless it appears
   in the NonReservedKeyWord() production. */

<DEFAULT, DQID, BTID> TOKEN :
{
    < A: "A" >
|   < ABS: "ABS" >
|   < ABSENT: "ABSENT" >
|   < ABSOLUTE: "ABSOLUTE" >
|   < ACTION: "ACTION" >
|   < ADA: "ADA" >
|   < ADD: "ADD" >
|   < ADMIN: "ADMIN" >
|   < AFTER: "AFTER" >
|   < ALL: "ALL" >
|   < ALLOCATE: "ALLOCATE" >
|   < ALLOW: "ALLOW" >
|   < ALTER: "ALTER" >
|   < ALWAYS: "ALWAYS" >
|   < AND: "AND" >
|   < ANY: "ANY" >
|   < APPLY: "APPLY" >
|   < ARE: "ARE" >
|   < ARRAY: "ARRAY" >
|   < ARRAY_MAX_CARDINALITY: "ARRAY_MAX_CARDINALITY" >
|   < AS: "AS" >
|   < ASC: "ASC" >
|   < ASENSITIVE: "ASENSITIVE" >
|   < ASSERTION: "ASSERTION" >
|   < ASSIGNMENT: "ASSIGNMENT" >
|   < ASYMMETRIC: "ASYMMETRIC" >
|   < AT: "AT" >
|   < ATOMIC: "ATOMIC" >
|   < ATTRIBUTE: "ATTRIBUTE" >
|   < ATTRIBUTES: "ATTRIBUTES" >
|   < AUTHORIZATION: "AUTHORIZATION" >
|   < AVG: "AVG" >
|   < BEFORE: "BEFORE" >
|   < BEGIN: "BEGIN" >
|   < BEGIN_FRAME: "BEGIN_FRAME" >
|   < BEGIN_PARTITION: "BEGIN_PARTITION" >
|   < BERNOULLI: "BERNOULLI" >
|   < BETWEEN: "BETWEEN" >
|   < BIGINT: "BIGINT" >
|   < BINARY: "BINARY" >
|   < BIT: "BIT" >
|   < BLOB: "BLOB" >
|   < BOOLEAN: "BOOLEAN" >
|   < BOTH: "BOTH" >
|   < BREADTH: "BREADTH" >
|   < BY: "BY" >
|   < C: "C" >
|   < CALL: "CALL" >
|   < CALLED: "CALLED" >
|   < CARDINALITY: "CARDINALITY" >
|   < CASCADE: "CASCADE" >
|   < CASCADED: "CASCADED" >
|   < CASE: "CASE" >
|   < CAST: "CAST" >
|   < CATALOG: "CATALOG" >
|   < CATALOG_NAME: "CATALOG_NAME" >
|   < CEIL: "CEIL" >
|   < CEILING: "CEILING" >
|   < CENTURY: "CENTURY" >
|   < CHAIN: "CHAIN" >
|   < CHAR: "CHAR" >
|   < CHAR_LENGTH: "CHAR_LENGTH" >
|   < CHARACTER: "CHARACTER" >
|   < CHARACTER_LENGTH: "CHARACTER_LENGTH" >
|   < CHARACTER_SET_CATALOG: "CHARACTER_SET_CATALOG" >
|   < CHARACTER_SET_NAME: "CHARACTER_SET_NAME" >
|   < CHARACTER_SET_SCHEMA: "CHARACTER_SET_SCHEMA" >
|   < CHARACTERISTICS: "CHARACTERISTICS" >
|   < CHARACTERS: "CHARACTERS" >
|   < CHECK: "CHECK" >
|   < CLASSIFIER: "CLASSIFIER" >
|   < CLASS_ORIGIN: "CLASS_ORIGIN" >
|   < CLOB: "CLOB" >
|   < CLOSE: "CLOSE" >
|   < COALESCE: "COALESCE" >
|   < COBOL: "COBOL" >
|   < COLLATE: "COLLATE" >
|   < COLLATION: "COLLATION" >
|   < COLLATION_CATALOG: "COLLATION_CATALOG" >
|   < COLLATION_NAME: "COLLATION_NAME" >
|   < COLLATION_SCHEMA: "COLLATION_SCHEMA" >
|   < COLLECT: "COLLECT" >
|   < COLUMN: "COLUMN" >
|   < COLUMN_NAME: "COLUMN_NAME" >
|   < COMMAND_FUNCTION: "COMMAND_FUNCTION" >
|   < COMMAND_FUNCTION_CODE: "COMMAND_FUNCTION_CODE" >
|   < COMMIT: "COMMIT" >
|   < COMMITTED: "COMMITTED" >
|   < CONDITION: "CONDITION" >
|   < CONDITIONAL: "CONDITIONAL" >
|   < CONDITION_NUMBER: "CONDITION_NUMBER" >
|   < CONNECT: "CONNECT" >
|   < CONNECTION: "CONNECTION" >
|   < CONNECTION_NAME: "CONNECTION_NAME" >
|   < CONSTRAINT: "CONSTRAINT" >
|   < CONSTRAINT_CATALOG: "CONSTRAINT_CATALOG" >
|   < CONSTRAINT_NAME: "CONSTRAINT_NAME" >
|   < CONSTRAINT_SCHEMA: "CONSTRAINT_SCHEMA" >
|   < CONSTRAINTS: "CONSTRAINTS" >
|   < CONSTRUCTOR: "CONSTRUCTOR" >
|   < CONTAINS: "CONTAINS" >
|   < CONTINUE: "CONTINUE" >
|   < CONVERT: "CONVERT" >
|   < CORR: "CORR" >
|   < CORRESPONDING: "CORRESPONDING" >
|   < COUNT: "COUNT" >
|   < COVAR_POP: "COVAR_POP" >
|   < COVAR_SAMP: "COVAR_SAMP" >
|   < CREATE: "CREATE" >
|   < CROSS: "CROSS" >
|   < CUBE: "CUBE" >
|   < CUME_DIST: "CUME_DIST" >
|   < CURRENT: "CURRENT" >
|   < CURRENT_CATALOG: "CURRENT_CATALOG" >
|   < CURRENT_DATE: "CURRENT_DATE" >
|   < CURRENT_DEFAULT_TRANSFORM_GROUP: "CURRENT_DEFAULT_TRANSFORM_GROUP" >
|   < CURRENT_PATH: "CURRENT_PATH" >
|   < CURRENT_ROLE: "CURRENT_ROLE" >
|   < CURRENT_ROW: "CURRENT_ROW" >
|   < CURRENT_SCHEMA: "CURRENT_SCHEMA" >
|   < CURRENT_TIME: "CURRENT_TIME" >
|   < CURRENT_TIMESTAMP: "CURRENT_TIMESTAMP" >
|   < CURRENT_TRANSFORM_GROUP_FOR_TYPE: "CURRENT_TRANSFORM_GROUP_FOR_TYPE" >
|   < CURRENT_USER: "CURRENT_USER" >
|   < CURSOR: "CURSOR" >
|   < CURSOR_NAME: "CURSOR_NAME" >
|   < CYCLE: "CYCLE" >
|   < DATA: "DATA" >
|   < DATABASE: "DATABASE" >
|   < DATE: "DATE" >
|   < DATETIME_INTERVAL_CODE: "DATETIME_INTERVAL_CODE" >
|   < DATETIME_INTERVAL_PRECISION: "DATETIME_INTERVAL_PRECISION" >
|   < DAY: "DAY" >
|   < DAYS: "DAYS" >
|   < DEALLOCATE: "DEALLOCATE" >
|   < DEC: "DEC" >
|   < DECADE: "DECADE" >
|   < DECIMAL: "DECIMAL" >
|   < DECLARE: "DECLARE" >
|   < DEFAULT_: "DEFAULT" >
|   < DEFAULTS: "DEFAULTS" >
|   < DEFERRABLE: "DEFERRABLE" >
|   < DEFERRED: "DEFERRED" >
|   < DEFINE: "DEFINE" >
|   < DEFINED: "DEFINED" >
|   < DEFINER: "DEFINER" >
|   < DEGREE: "DEGREE" >
|   < DELETE: "DELETE" >
|   < DENSE_RANK: "DENSE_RANK" >
|   < DEPTH: "DEPTH" >
|   < DEREF: "DEREF" >
|   < DERIVED: "DERIVED" >
|   < DESC: "DESC" >
|   < DESCRIBE: "DESCRIBE" >
|   < DESCRIPTION: "DESCRIPTION" >
|   < DESCRIPTOR: "DESCRIPTOR" >
|   < DETERMINISTIC: "DETERMINISTIC" >
|   < DIAGNOSTICS: "DIAGNOSTICS" >
|   < DISALLOW: "DISALLOW" >
|   < DISCONNECT: "DISCONNECT" >
|   < DISPATCH: "DISPATCH" >
|   < DISTINCT: "DISTINCT" >
|   < DOMAIN: "DOMAIN" >
|   < DOUBLE: "DOUBLE" >
|   < DOW: "DOW" >
|   < DOY: "DOY" >
|   < DROP: "DROP" >
|   < DYNAMIC: "DYNAMIC" >
|   < DYNAMIC_FUNCTION: "DYNAMIC_FUNCTION" >
|   < DYNAMIC_FUNCTION_CODE: "DYNAMIC_FUNCTION_CODE" >
|   < EACH: "EACH" >
|   < ELEMENT: "ELEMENT" >
|   < ELSE: "ELSE" >
|   < EMPTY: "EMPTY" >
|   < ENCODING: "ENCODING">
|   < END: "END" >
|   < END_EXEC: "END-EXEC" >
|   < END_FRAME: "END_FRAME" >
|   < END_PARTITION: "END_PARTITION" >
|   < EPOCH: "EPOCH" >
|   < EQUALS: "EQUALS" >
|   < ERROR: "ERROR" >
|   < ESCAPE: "ESCAPE" >
|   < EVERY: "EVERY" >
|   < EXCEPT: "EXCEPT" >
|   < EXCEPTION: "EXCEPTION" >
|   < EXCLUDE: "EXCLUDE" >
|   < EXCLUDING: "EXCLUDING" >
|   < EXEC: "EXEC" >
|   < EXECUTE: "EXECUTE" >
|   < EXISTS: "EXISTS" >
|   < EXP: "EXP" >
|   < EXPLAIN: "EXPLAIN" >
|   < EXTEND: "EXTEND" >
|   < EXTERNAL: "EXTERNAL" >
|   < EXTRACT: "EXTRACT" >
|   < FALSE: "FALSE" >
|   < FETCH: "FETCH" >
|   < FILTER: "FILTER" >
|   < FINAL: "FINAL" >
|   < FIRST: "FIRST" >
|   < FIRST_VALUE: "FIRST_VALUE">
|   < FLOAT: "FLOAT" >
|   < FLOOR: "FLOOR" >
|   < FOLLOWING: "FOLLOWING" >
|   < FOR: "FOR" >
|   < FORMAT: "FORMAT" >
|   < FOREIGN: "FOREIGN" >
|   < FORTRAN: "FORTRAN" >
|   < FOUND: "FOUND" >
|   < FRAC_SECOND: "FRAC_SECOND" >
|   < FRAME_ROW: "FRAME_ROW" >
|   < FREE: "FREE" >
|   < FROM: "FROM" >
|   < FULL: "FULL" >
|   < FUNCTION: "FUNCTION" >
|   < FUSION: "FUSION" >
|   < G: "G" >
|   < GENERAL: "GENERAL" >
|   < GENERATED: "GENERATED" >
|   < GEOMETRY: "GEOMETRY" >
|   < GET: "GET" >
|   < GLOBAL: "GLOBAL" >
|   < GO: "GO" >
|   < GOTO: "GOTO" >
|   < GRANT: "GRANT" >
|   < GRANTED: "GRANTED" >
|   < GROUP: "GROUP" >
|   < GROUPING: "GROUPING" >
|   < GROUPS: "GROUPS" >
|   < HAVING: "HAVING" >
|   < HIERARCHY: "HIERARCHY" >
|   < HOLD: "HOLD" >
|   < HOP: "HOP" >
|   < HOUR: "HOUR" >
|   < HOURS: "HOURS" >
|   < IDENTITY: "IDENTITY" >
|   < IGNORE: "IGNORE" >
|   < IMMEDIATE: "IMMEDIATE" >
|   < IMMEDIATELY: "IMMEDIATELY" >
|   < IMPLEMENTATION: "IMPLEMENTATION" >
|   < IMPORT: "IMPORT" >
|   < IN: "IN" >
|   < INCLUDING: "INCLUDING" >
|   < INCREMENT: "INCREMENT" >
|   < INDICATOR: "INDICATOR" >
|   < INITIAL: "INITIAL" >
|   < INITIALLY: "INITIALLY" >
|   < INNER: "INNER" >
|   < INOUT: "INOUT" >
|   < INPUT: "INPUT" >
|   < INSENSITIVE: "INSENSITIVE" >
|   < INSERT: "INSERT" >
|   < INSTANCE: "INSTANCE" >
|   < INSTANTIABLE: "INSTANTIABLE" >
|   < INT: "INT" >
|   < INTEGER: "INTEGER" >
|   < INTERSECT: "INTERSECT" >
|   < INTERSECTION: "INTERSECTION" >
|   < INTERVAL: "INTERVAL" >
|   < INTO: "INTO" >
|   < INVOKER: "INVOKER" >
|   < IS: "IS" >
|   < ISODOW: "ISODOW" >
|   < ISOYEAR: "ISOYEAR" >
|   < ISOLATION: "ISOLATION" >
|   < JAVA: "JAVA" >
|   < JOIN: "JOIN" >
|   < JSON: "JSON" >
|   < JSON_ARRAY: "JSON_ARRAY">
|   < JSON_ARRAYAGG: "JSON_ARRAYAGG">
|   < JSON_EXISTS: "JSON_EXISTS" >
|   < JSON_OBJECT: "JSON_OBJECT">
|   < JSON_OBJECTAGG: "JSON_OBJECTAGG">
|   < JSON_QUERY: "JSON_QUERY" >
|   < JSON_VALUE: "JSON_VALUE" >
|   < K: "K" >
|   < KEY: "KEY" >
|   < KEY_MEMBER: "KEY_MEMBER" >
|   < KEY_TYPE: "KEY_TYPE" >
|   < LABEL: "LABEL" >
|   < LAG: "LAG" >
|   < LANGUAGE: "LANGUAGE" >
|   < LARGE: "LARGE" >
|   < LAST: "LAST" >
|   < LAST_VALUE: "LAST_VALUE" >
|   < LATERAL: "LATERAL" >
|   < LEAD: "LEAD" >
|   < LEADING: "LEADING" >
|   < LEFT: "LEFT" >
|   < LENGTH: "LENGTH" >
|   < LEVEL: "LEVEL" >
|   < LIBRARY: "LIBRARY" >
|   < LIKE: "LIKE" >
|   < LIKE_REGEX: "LIKE_REGEX" >
|   < LIMIT: "LIMIT" >
|   < LN: "LN" >
|   < LOCAL: "LOCAL" >
|   < LOCALTIME: "LOCALTIME" >
|   < LOCALTIMESTAMP: "LOCALTIMESTAMP" >
|   < LOCATOR: "LOCATOR" >
|   < LOWER: "LOWER" >
|   < M: "M" >
|   < MAP: "MAP" >
|   < MATCH: "MATCH" >
|   < MATCHED: "MATCHED" >
|   < MATCHES: "MATCHES" >
|   < MATCH_NUMBER: "MATCH_NUMBER">
|   < MATCH_RECOGNIZE: "MATCH_RECOGNIZE">
|   < MAX: "MAX" >
|   < MAXVALUE: "MAXVALUE" >
|   < MEASURES: "MEASURES" >
|   < MEMBER: "MEMBER" >
|   < MERGE: "MERGE" >
|   < MESSAGE_LENGTH: "MESSAGE_LENGTH" >
|   < MESSAGE_OCTET_LENGTH: "MESSAGE_OCTET_LENGTH" >
|   < MESSAGE_TEXT: "MESSAGE_TEXT" >
|   < METHOD: "METHOD" >
|   < MICROSECOND: "MICROSECOND" >
|   < MILLISECOND: "MILLISECOND" >
|   < MILLENNIUM: "MILLENNIUM" >
|   < MIN: "MIN" >
|   < MINUTE: "MINUTE" >
|   < MINUTES: "MINUTES" >
|   < MINVALUE: "MINVALUE" >
|   < MOD: "MOD" >
|   < MODIFIES: "MODIFIES" >
|   < MODULE: "MODULE" >
|   < MONTH: "MONTH" >
|   < MONTHS: "MONTHS" >
|   < MORE_: "MORE" >
|   < MULTISET: "MULTISET" >
|   < MUMPS: "MUMPS" >
|   < NAME: "NAME" >
|   < NAMES: "NAMES" >
|   < NANOSECOND: "NANOSECOND" >
|   < NATIONAL: "NATIONAL" >
|   < NATURAL: "NATURAL" >
|   < NCHAR: "NCHAR" >
|   < NCLOB: "NCLOB" >
|   < NESTING: "NESTING" >
|   < NEW: "NEW" >
|   < NEXT: "NEXT" >
|   < NO: "NO" >
|   < NONE: "NONE" >
|   < NORMALIZE: "NORMALIZE" >
|   < NORMALIZED: "NORMALIZED" >
|   < NOT: "NOT" >
|   < NTH_VALUE: "NTH_VALUE" >
|   < NTILE: "NTILE" >
|   < NULL: "NULL" >
|   < NULLABLE: "NULLABLE" >
|   < NULLIF: "NULLIF" >
|   < NULLS: "NULLS" >
|   < NUMBER: "NUMBER" >
|   < NUMERIC: "NUMERIC" >
|   < OBJECT: "OBJECT" >
|   < OCCURRENCES_REGEX: "OCCURRENCES_REGEX" >
|   < OCTET_LENGTH: "OCTET_LENGTH" >
|   < OCTETS: "OCTETS" >
|   < OF: "OF" >
|   < OFFSET: "OFFSET" >
|   < OLD: "OLD" >
|   < OMIT: "OMIT" >
|   < ON: "ON" >
|   < ONE: "ONE" >
|   < ONLY: "ONLY" >
|   < OPEN: "OPEN" >
|   < OPTION: "OPTION" >
|   < OPTIONS: "OPTIONS" >
|   < OR: "OR" >
|   < ORDER: "ORDER" >
|   < ORDERING: "ORDERING" >
|   < ORDINALITY: "ORDINALITY" >
|   < OTHERS: "OTHERS" >
|   < OUT: "OUT" >
|   < OUTER: "OUTER" >
|   < OUTPUT: "OUTPUT" >
|   < OVER: "OVER" >
|   < OVERLAPS: "OVERLAPS" >
|   < OVERLAY: "OVERLAY" >
|   < OVERRIDING: "OVERRIDING" >
|   < PAD: "PAD" >
|   < PARAMETER: "PARAMETER" >
|   < PARAMETER_MODE: "PARAMETER_MODE" >
|   < PARAMETER_NAME: "PARAMETER_NAME" >
|   < PARAMETER_ORDINAL_POSITION: "PARAMETER_ORDINAL_POSITION" >
|   < PARAMETER_SPECIFIC_CATALOG: "PARAMETER_SPECIFIC_CATALOG" >
|   < PARAMETER_SPECIFIC_NAME: "PARAMETER_SPECIFIC_NAME" >
|   < PARAMETER_SPECIFIC_SCHEMA: "PARAMETER_SPECIFIC_SCHEMA" >
|   < PARTIAL: "PARTIAL" >
|   < PARTITION: "PARTITION" >
|   < PASCAL: "PASCAL" >
|   < PASSING: "PASSING" >
|   < PASSTHROUGH: "PASSTHROUGH" >
|   < PAST: "PAST" >
|   < PATH: "PATH" >
|   < PATTERN: "PATTERN" >
|   < PER: "PER" >
|   < PERCENT: "PERCENT" >
|   < PERCENTILE_CONT: "PERCENTILE_CONT" >
|   < PERCENTILE_DISC: "PERCENTILE_DISC" >
|   < PERCENT_RANK: "PERCENT_RANK" >
|   < PERIOD: "PERIOD" >
|   < PERMUTE: "PERMUTE" >
|   < PLACING: "PLACING" >
|   < PLAN: "PLAN" >
|   < PLI: "PLI" >
|   < PORTION: "PORTION" >
|   < POSITION: "POSITION" >
|   < POSITION_REGEX: "POSITION_REGEX" >
|   < POWER: "POWER" >
|   < PRECEDES: "PRECEDES" >
|   < PRECEDING: "PRECEDING" >
|   < PRECISION: "PRECISION" >
|   < PREPARE: "PREPARE" >
|   < PRESERVE: "PRESERVE" >
|   < PREV: "PREV" >
|   < PRIMARY: "PRIMARY" >
|   < PRIOR: "PRIOR" >
|   < PRIVILEGES: "PRIVILEGES" >
|   < PROCEDURE: "PROCEDURE" >
|   < PUBLIC: "PUBLIC" >
|   < QUARTER: "QUARTER" >
|   < RANGE: "RANGE" >
|   < RANK: "RANK" >
|   < READ: "READ" >
|   < READS: "READS" >
|   < REAL: "REAL" >
|   < RECURSIVE: "RECURSIVE" >
|   < REF: "REF" >
|   < REFERENCES: "REFERENCES" >
|   < REFERENCING: "REFERENCING" >
|   < REGR_AVGX: "REGR_AVGX" >
|   < REGR_AVGY: "REGR_AVGY" >
|   < REGR_COUNT: "REGR_COUNT" >
|   < REGR_INTERCEPT: "REGR_INTERCEPT" >
|   < REGR_R2: "REGR_R2" >
|   < REGR_SLOPE: "REGR_SLOPE" >
|   < REGR_SXX: "REGR_SXX" >
|   < REGR_SXY: "REGR_SXY" >
|   < REGR_SYY: "REGR_SYY" >
|   < RELATIVE: "RELATIVE" >
|   < RELEASE: "RELEASE" >
|   < REPEATABLE: "REPEATABLE" >
|   < REPLACE: "REPLACE" >
|   < RESET: "RESET" >
|   < RESPECT: "RESPECT" >
|   < RESTART: "RESTART" >
|   < RESTRICT: "RESTRICT" >
|   < RESULT: "RESULT" >
|   < RETURN: "RETURN" >
|   < RETURNED_CARDINALITY: "RETURNED_CARDINALITY" >
|   < RETURNED_LENGTH: "RETURNED_LENGTH" >
|   < RETURNED_OCTET_LENGTH: "RETURNED_OCTET_LENGTH" >
|   < RETURNED_SQLSTATE: "RETURNED_SQLSTATE" >
|   < RETURNING: "RETURNING" >
|   < RETURNS: "RETURNS" >
|   < REVOKE: "REVOKE" >
|   < RIGHT: "RIGHT" >
|   < ROLE: "ROLE" >
|   < ROLLBACK: "ROLLBACK" >
|   < ROLLUP: "ROLLUP" >
|   < ROUTINE: "ROUTINE" >
|   < ROUTINE_CATALOG: "ROUTINE_CATALOG" >
|   < ROUTINE_NAME: "ROUTINE_NAME" >
|   < ROUTINE_SCHEMA: "ROUTINE_SCHEMA" >
|   < ROW: "ROW" >
|   < ROW_COUNT: "ROW_COUNT" >
|   < ROW_NUMBER: "ROW_NUMBER" >
|   < ROWS: "ROWS" >
|   < RUNNING: "RUNNING" >
|   < SAVEPOINT: "SAVEPOINT" >
|   < SCALAR: "SCALAR" >
|   < SCALE: "SCALE" >
|   < SCHEMA: "SCHEMA" >
|   < SCHEMA_NAME: "SCHEMA_NAME" >
|   < SCOPE: "SCOPE" >
|   < SCOPE_CATALOGS: "SCOPE_CATALOGS" >
|   < SCOPE_NAME: "SCOPE_NAME" >
|   < SCOPE_SCHEMA: "SCOPE_SCHEMA" >
|   < SCROLL: "SCROLL" >
|   < SEARCH: "SEARCH" >
|   < SECOND: "SECOND" >
|   < SECONDS: "SECONDS" >
|   < SECTION: "SECTION" >
|   < SECURITY: "SECURITY" >
|   < SEEK: "SEEK" >
|   < SELECT: "SELECT" >
|   < SELF: "SELF" >
|   < SENSITIVE: "SENSITIVE" >
|   < SEQUENCE: "SEQUENCE" >
|   < SERIALIZABLE: "SERIALIZABLE" >
|   < SERVER: "SERVER" >
|   < SERVER_NAME: "SERVER_NAME" >
|   < SESSION: "SESSION" >
|   < SESSION_USER: "SESSION_USER" >
|   < SET: "SET" >
|   < SETS: "SETS" >
|   < SET_MINUS: "MINUS">
|   < SHOW: "SHOW" >
|   < SIMILAR: "SIMILAR" >
|   < SIMPLE: "SIMPLE" >
|   < SIZE: "SIZE" >
|   < SKIP_: "SKIP" >
|   < SMALLINT: "SMALLINT" >
|   < SOME: "SOME" >
|   < SOURCE: "SOURCE" >
|   < SPACE: "SPACE" >
|   < SPECIFIC: "SPECIFIC" >
|   < SPECIFIC_NAME: "SPECIFIC_NAME" >
|   < SPECIFICTYPE: "SPECIFICTYPE" >
|   < SQL: "SQL" >
|   < SQLEXCEPTION: "SQLEXCEPTION" >
|   < SQLSTATE: "SQLSTATE" >
|   < SQLWARNING: "SQLWARNING" >
|   < SQL_BIGINT: "SQL_BIGINT" >
|   < SQL_BINARY: "SQL_BINARY" >
|   < SQL_BIT: "SQL_BIT" >
|   < SQL_BLOB: "SQL_BLOB" >
|   < SQL_BOOLEAN: "SQL_BOOLEAN" >
|   < SQL_CHAR: "SQL_CHAR" >
|   < SQL_CLOB: "SQL_CLOB" >
|   < SQL_DATE: "SQL_DATE" >
|   < SQL_DECIMAL: "SQL_DECIMAL" >
|   < SQL_DOUBLE: "SQL_DOUBLE" >
|   < SQL_FLOAT: "SQL_FLOAT" >
|   < SQL_INTEGER: "SQL_INTEGER" >
|   < SQL_INTERVAL_DAY: "SQL_INTERVAL_DAY" >
|   < SQL_INTERVAL_DAY_TO_HOUR: "SQL_INTERVAL_DAY_TO_HOUR" >
|   < SQL_INTERVAL_DAY_TO_MINUTE: "SQL_INTERVAL_DAY_TO_MINUTE" >
|   < SQL_INTERVAL_DAY_TO_SECOND: "SQL_INTERVAL_DAY_TO_SECOND" >
|   < SQL_INTERVAL_HOUR: "SQL_INTERVAL_HOUR" >
|   < SQL_INTERVAL_HOUR_TO_MINUTE: "SQL_INTERVAL_HOUR_TO_MINUTE" >
|   < SQL_INTERVAL_HOUR_TO_SECOND: "SQL_INTERVAL_HOUR_TO_SECOND" >
|   < SQL_INTERVAL_MINUTE: "SQL_INTERVAL_MINUTE" >
|   < SQL_INTERVAL_MINUTE_TO_SECOND: "SQL_INTERVAL_MINUTE_TO_SECOND" >
|   < SQL_INTERVAL_MONTH: "SQL_INTERVAL_MONTH" >
|   < SQL_INTERVAL_SECOND: "SQL_INTERVAL_SECOND" >
|   < SQL_INTERVAL_YEAR: "SQL_INTERVAL_YEAR" >
|   < SQL_INTERVAL_YEAR_TO_MONTH: "SQL_INTERVAL_YEAR_TO_MONTH" >
|   < SQL_LONGVARBINARY: "SQL_LONGVARBINARY" >
|   < SQL_LONGVARCHAR: "SQL_LONGVARCHAR" >
|   < SQL_LONGVARNCHAR: "SQL_LONGVARNCHAR" >
|   < SQL_NCHAR: "SQL_NCHAR" >
|   < SQL_NCLOB: "SQL_NCLOB" >
|   < SQL_NUMERIC: "SQL_NUMERIC" >
|   < SQL_NVARCHAR: "SQL_NVARCHAR" >
|   < SQL_REAL: "SQL_REAL" >
|   < SQL_SMALLINT: "SQL_SMALLINT" >
|   < SQL_TIME: "SQL_TIME" >
|   < SQL_TIMESTAMP: "SQL_TIMESTAMP" >
|   < SQL_TINYINT: "SQL_TINYINT" >
|   < SQL_TSI_DAY: "SQL_TSI_DAY" >
|   < SQL_TSI_FRAC_SECOND: "SQL_TSI_FRAC_SECOND" >
|   < SQL_TSI_HOUR: "SQL_TSI_HOUR" >
|   < SQL_TSI_MICROSECOND: "SQL_TSI_MICROSECOND" >
|   < SQL_TSI_MINUTE: "SQL_TSI_MINUTE" >
|   < SQL_TSI_MONTH: "SQL_TSI_MONTH" >
|   < SQL_TSI_QUARTER: "SQL_TSI_QUARTER" >
|   < SQL_TSI_SECOND: "SQL_TSI_SECOND" >
|   < SQL_TSI_WEEK: "SQL_TSI_WEEK" >
|   < SQL_TSI_YEAR: "SQL_TSI_YEAR" >
|   < SQL_VARBINARY: "SQL_VARBINARY" >
|   < SQL_VARCHAR: "SQL_VARCHAR" >
|   < SQRT: "SQRT" >
|   < START: "START" >
|   < STATE: "STATE" >
|   < STATEMENT: "STATEMENT" >
|   < STATIC: "STATIC" >
|   < STDDEV_POP: "STDDEV_POP" >
|   < STDDEV_SAMP: "STDDEV_SAMP" >
|   < STREAM: "STREAM" >
|   < STRUCTURE: "STRUCTURE" >
|   < STYLE: "STYLE" >
|   < SUBCLASS_ORIGIN: "SUBCLASS_ORIGIN" >
|   < SUBMULTISET: "SUBMULTISET" >
|   < SUBSET: "SUBSET" >
|   < SUBSTITUTE: "SUBSTITUTE" >
|   < SUBSTRING: "SUBSTRING" >
|   < SUBSTRING_REGEX: "SUBSTRING_REGEX" >
|   < SUCCEEDS: "SUCCEEDS" >
|   < SUM: "SUM" >
|   < SYMMETRIC: "SYMMETRIC" >
|   < SYSTEM: "SYSTEM" >
|   < SYSTEM_TIME: "SYSTEM_TIME" >
|   < SYSTEM_USER: "SYSTEM_USER" >
|   < TABLE: "TABLE" >
|   < TABLE_NAME: "TABLE_NAME" >
|   < TABLESAMPLE: "TABLESAMPLE" >
|   < TEMPORARY: "TEMPORARY" >
|   < THEN: "THEN" >
|   < TIES: "TIES" >
|   < TIME: "TIME" >
|   < TIMESTAMP: "TIMESTAMP" >
|   < TIMESTAMPADD: "TIMESTAMPADD" >
|   < TIMESTAMPDIFF: "TIMESTAMPDIFF" >
|   < TIMEZONE_HOUR: "TIMEZONE_HOUR" >
|   < TIMEZONE_MINUTE: "TIMEZONE_MINUTE" >
|   < TINYINT: "TINYINT" >
|   < TO: "TO" >
|   < TOP_LEVEL_COUNT: "TOP_LEVEL_COUNT" >
|   < TRAILING: "TRAILING" >
|   < TRANSACTION: "TRANSACTION" >
|   < TRANSACTIONS_ACTIVE: "TRANSACTIONS_ACTIVE" >
|   < TRANSACTIONS_COMMITTED: "TRANSACTIONS_COMMITTED" >
|   < TRANSACTIONS_ROLLED_BACK: "TRANSACTIONS_ROLLED_BACK" >
|   < TRANSFORM: "TRANSFORM" >
|   < TRANSFORMS: "TRANSFORMS" >
|   < TRANSLATE: "TRANSLATE" >
|   < TRANSLATE_REGEX: "TRANSLATE_REGEX" >
|   < TRANSLATION: "TRANSLATION" >
|   < TREAT: "TREAT" >
|   < TRIGGER: "TRIGGER" >
|   < TRIGGER_CATALOG: "TRIGGER_CATALOG" >
|   < TRIGGER_NAME: "TRIGGER_NAME" >
|   < TRIGGER_SCHEMA: "TRIGGER_SCHEMA" >
|   < TRIM: "TRIM" >
|   < TRIM_ARRAY: "TRIM_ARRAY" >
|   < TRUE: "TRUE" >
|   < TRUNCATE: "TRUNCATE" >
|   < TUMBLE: "TUMBLE" >
|   < TYPE: "TYPE" >
|   < UESCAPE: "UESCAPE" >
|   < UNBOUNDED: "UNBOUNDED" >
|   < UNCOMMITTED: "UNCOMMITTED" >
|   < UNCONDITIONAL: "UNCONDITIONAL" >
|   < UNDER: "UNDER" >
|   < UNION: "UNION" >
|   < UNIQUE: "UNIQUE" >
|   < UNKNOWN: "UNKNOWN" >
|   < UNNAMED: "UNNAMED" >
|   < UNNEST: "UNNEST" >
|   < UPDATE: "UPDATE" >
|   < UPPER: "UPPER" >
|   < UPSERT: "UPSERT" >
|   < USAGE: "USAGE" >
|   < USER: "USER" >
|   < USER_DEFINED_TYPE_CATALOG: "USER_DEFINED_TYPE_CATALOG" >
|   < USER_DEFINED_TYPE_CODE: "USER_DEFINED_TYPE_CODE" >
|   < USER_DEFINED_TYPE_NAME: "USER_DEFINED_TYPE_NAME" >
|   < USER_DEFINED_TYPE_SCHEMA: "USER_DEFINED_TYPE_SCHEMA" >
|   < USING: "USING" >
|   < UTF8: "UTF8" >
|   < UTF16: "UTF16" >
|   < UTF32: "UTF32" >
|   < VALUE: "VALUE" >
|   < VALUES: "VALUES" >
|   < VALUE_OF: "VALUE_OF" >
|   < VAR_POP: "VAR_POP" >
|   < VAR_SAMP: "VAR_SAMP" >
|   < VARBINARY: "VARBINARY" >
|   < VARCHAR: "VARCHAR" >
|   < VARYING: "VARYING" >
|   < VERSION: "VERSION" >
|   < VERSIONING: "VERSIONING" >
|   < VIEW: "VIEW" >
|   < WEEK: "WEEK" >
|   < WHEN: "WHEN" >
|   < WHENEVER: "WHENEVER" >
|   < WHERE: "WHERE" >
|   < WIDTH_BUCKET: "WIDTH_BUCKET" >
|   < WINDOW: "WINDOW" >
|   < WITH: "WITH" >
|   < WITHIN: "WITHIN" >
|   < WITHOUT: "WITHOUT" >
|   < WORK: "WORK" >
|   < WRAPPER: "WRAPPER" >
|   < WRITE: "WRITE" >
|   < XML: "XML" >
|   < YEAR: "YEAR" >
|   < YEARS: "YEARS" >
|   < ZONE: "ZONE" >
<#-- additional parser keywords are included here -->
<#list parser.keywords as keyword>
|   < ${keyword}: "${keyword}" >
</#list>
}

/**
 * Parses a non-reserved keyword for use as an identifier.
 *
 * <p>The method is broken up into several sub-methods; without this
 * decomposition, parsers such as Dialect1 with more than ~1,000 non-reserved
 * keywords would generate such deeply nested 'if' statements that javac would
 * fail with a {@link StackOverflowError}.
 *
 * <p>The list is generated from the FMPP config data. To add or remove
 * keywords, modify config.fmpp. For parsers except Babel, make sure that
 * keywords are not reserved by the SQL standard.
 *
 * @see Glossary#SQL2003 SQL:2003 Part 2 Section 5.2
 */
String NonReservedKeyWord() :
{
}
{
    (
        NonReservedKeyWord0of3()
    |   NonReservedKeyWord1of3()
    |   NonReservedKeyWord2of3()
    )
    {
        return unquotedIdentifier();
    }
}

/** @see #NonReservedKeyWord */
void NonReservedKeyWord0of3() :
{
}
{
    (
<#list parser.nonReservedKeywords + parser.nonReservedKeywordsToAdd as keyword>
<#if keyword?index == 0>
        <${keyword}>
<#elseif keyword?index % 3 == 0>
    |   <${keyword}>
</#if>
</#list>
    )
}

/** @see #NonReservedKeyWord */
void NonReservedKeyWord1of3() :
{
}
{
    (
<#list parser.nonReservedKeywords + parser.nonReservedKeywordsToAdd as keyword>
<#if keyword?index == 1>
        <${keyword}>
<#elseif keyword?index % 3 == 1>
    |   <${keyword}>
</#if>
</#list>
    )
}

/** @see #NonReservedKeyWord */
void NonReservedKeyWord2of3() :
{
}
{
    (
<#list parser.nonReservedKeywords + parser.nonReservedKeywordsToAdd as keyword>
<#if keyword?index == 2>
        <${keyword}>
<#elseif keyword?index % 3 == 2>
    |   <${keyword}>
</#if>
</#list>
    )
}

/**
 * Defines a production which can never be accepted by the parser.
 * In effect, it tells the parser, "If you got here, you've gone too far."
 * It is used as the default production for parser extension points;
 * derived parsers replace it with a real production when they want to
 * implement a particular extension point.
 */
void UnusedExtension() :
{
}
{
    (
        LOOKAHEAD({false}) <ZONE>
    )
}
