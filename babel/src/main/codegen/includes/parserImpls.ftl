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

boolean IsVolatileOpt() :
{
}
{
    <VOLATILE> { return true; }
|
    { return false; }
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

void ColumnWithType(List<SqlNode> list) :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    final Span s = Span.of();
}
{
    id = CompoundIdentifier()
    type = DataType()
    [
        <NOT> <NULL> {
            nullable = false;
        }
    ]
    {
        list.add(SqlDdlNodes.column(s.add(id).end(this), id, type.withNullable(nullable), null, null));
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final SetType setType;
    final boolean isVolatile;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList columnList;
    final SqlNode query;
}
{
    setType = SetTypeOpt() isVolatile = IsVolatileOpt() <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
        columnList = ExtendColumnList()
    |
        { columnList = null; }
    )
    (
        <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |
        { query = null; }
    )
    {
        return new SqlCreateTable(s.end(this), replace, setType, isVolatile, ifNotExists, id, columnList, query);
    }
}

/** 
    Reason for having this is to be able to return the SqlExecMacro class since Parser.jj does not have it as a reference. 
    This can also likely be extended to accomodate optional parameters later if need be.
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
