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
 * Parses an SQL statement.
 */
SqlNode SqlStmt() :
{
    SqlNode stmt;
}
{
    (
        stmt = SqlSetOption(Span.of(), null)
    |
        stmt = SqlAlter()
    |
        stmt = SqlCreate()
    |
        stmt = SqlUpdate()
    |
        stmt = SqlInsert()
    |
        stmt = SqlDrop()
    |
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
        create = SqlCreateFunction()
    )
    {
        return create;
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
        drop = SqlDropMaterializedView(s, replace)
    |
        drop = SqlDropSchema(s, replace)
    |
        drop = SqlDropTable(s, replace)
    |
        drop = SqlDropType(s, replace)
    |
        drop = SqlDropView(s, replace)
    |
        drop = SqlDropFunction(s, replace)
    )
    {
        return drop;
    }
}
