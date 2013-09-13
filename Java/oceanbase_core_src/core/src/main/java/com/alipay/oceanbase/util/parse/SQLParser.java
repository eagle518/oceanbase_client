package com.alipay.oceanbase.util.parse;

import java.sql.SQLException;
import java.util.regex.Pattern;

import com.alipay.oceanbase.util.StringUtils;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SQLParser.java, v 0.1 2013-5-24 下午4:05:18 liangjie.li Exp $
 */
public class SQLParser {
    static final Pattern SELECT_FOR_UPDATE_PATTERN = Pattern.compile(
                                                       "^select\\s+.*\\s+for\\s+update.*$",
                                                       Pattern.CASE_INSENSITIVE);

    public static SqlType getSqlType(String sql) throws SQLException {
        SqlType sqlType = null;
        //#bug 2011-12-8, modify by junyu, this code use huge cpu resource, and most sql have no comment, 
        // so first simple look for there whether have the comment
        String noCommentsSql = sql;
        if (sql.contains("/*")) {
            noCommentsSql = StringUtils.stripComments(sql, "'\"", "'\"", true, false, true, true)
                .trim();
        }

        int beginPos = 0;
        int sqlLen = noCommentsSql.length();
        while (beginPos < sqlLen && noCommentsSql.charAt(beginPos) == '(') {
            ++beginPos;
        }
        if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, "select", beginPos)) {
            //#bug 2011-12-9, this select-for-update regex has low performance, so first judge this sql whether have ' for ' string.
            if (noCommentsSql.toLowerCase().contains(" for ")
                && SELECT_FOR_UPDATE_PATTERN.matcher(noCommentsSql).matches()) {
                sqlType = SqlType.SELECT_FOR_UPDATE;
            } else {
                sqlType = SqlType.SELECT;
            }
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.SHOW.name())) {
            sqlType = SqlType.SHOW;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.INSERT.name())) {
            sqlType = SqlType.INSERT;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.UPDATE.name())) {
            sqlType = SqlType.UPDATE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.DELETE.name())) {
            sqlType = SqlType.DELETE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.REPLACE.name())) {
            sqlType = SqlType.REPLACE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.TRUNCATE.name())) {
            sqlType = SqlType.TRUNCATE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.CREATE.name())) {
            sqlType = SqlType.CREATE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.DROP.name())) {
            sqlType = SqlType.DROP;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.LOAD.name())) {
            sqlType = SqlType.LOAD;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.MERGE.name())) {
            sqlType = SqlType.MERGE;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.ALTER.name())) {
            sqlType = SqlType.ALTER;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.EXPLAIN.name())) {
            sqlType = SqlType.EXPLAIN;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.DESC.name())) {
            sqlType = SqlType.DESC;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.SET.name())) {
            sqlType = SqlType.SET;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(noCommentsSql, SqlType.RENAME.name())) {
            sqlType = SqlType.RENAME;
        } else {
            sqlType = SqlType.DEFAULT;
        }
        return sqlType;
    }

    private SQLParser() {
    }
}