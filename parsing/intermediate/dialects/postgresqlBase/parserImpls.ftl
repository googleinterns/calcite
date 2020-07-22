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
}
{
    //~ FUNCTIONS WITH SPECIAL SYNTAX ---------------------------------------
    (
        <CAST> { s = span(); }
        <LPAREN> e = Expression(ExprContext.ACCEPT_SUB_QUERY) { args = startList(e); }
        <AS>
        (
            dt = DataType() { args.add(dt); }
        |
            <INTERVAL> e = IntervalQualifier() { args.add(e); }
        )
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
                args.add(name);
            }
            <RPAREN> {
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
        <SUBSTRING>
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

/* OPERATORS */

<DEFAULT, DQID, BTID> TOKEN :
{
    < INFIX_CAST: \"::\" >
}
