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

SqlExcept ExceptExpression(List<SqlNode> selectList):
{
    final Pair<SqlNodeList, SqlNodeList> nameAndTypePair;
    final SqlNodeList exceptList;
}
{
    <EXCEPT> nameAndTypePair = ParenthesizedCompoundIdentifierList()
    {
        exceptList = nameAndTypePair.getKey();
        if (selectList.size() > 0) {
            SqlIdentifier identifier = (SqlIdentifier) selectList.get(0);
            if (!identifier.toString().equals("*") || selectList.size() != 1) {
                throw SqlUtil.newContextException(getPos(),
                    RESOURCE.illegalQueryExpression());
            }
        }
        return new SqlExcept(getPos(), exceptList);
    }
}

/**
 * Parses a leaf SELECT expression without ORDER BY.
 */
SqlSelect SqlSelect() :
{
    final List<SqlLiteral> keywords = new ArrayList<SqlLiteral>();
    final SqlNodeList keywordList;
    SqlExcept exceptExpression = null;
    List<SqlNode> selectList;
    final SqlNode fromClause;
    final SqlNode where;
    final SqlNodeList groupBy;
    final SqlNode having;
    final SqlNodeList windowDecls;
    final List<SqlNode> hints = new ArrayList<SqlNode>();
    final Span s;
}
{
    <SELECT>
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
    )?
    {
        keywordList = new SqlNodeList(keywords, s.addAll(keywords).pos());
    }
    selectList = SelectList()
    [
        exceptExpression = ExceptExpression(selectList)
    ]
    (
        <FROM> fromClause = FromClause()
        where = WhereOpt()
        groupBy = GroupByOpt()
        having = HavingOpt()
        windowDecls = WindowOpt()
    |
        E() {
            fromClause = null;
            where = null;
            groupBy = null;
            having = null;
            windowDecls = null;
        }
    )
    {
        return new SqlSelect(s.end(this), keywordList, /*topN=*/ null,
            new SqlNodeList(selectList, Span.of(selectList).pos()),
            exceptExpression, fromClause, where, groupBy, having, /*qualify=*/ null,
            windowDecls, /*orderBy=*/ null, /*offset=*/ null, /*fetch=*/ null,
            new SqlNodeList(hints, getPos()));
    }
}
