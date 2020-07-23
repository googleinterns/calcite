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
                InfixCast(list, exprContext, s)
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
