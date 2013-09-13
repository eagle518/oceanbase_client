package com.alipay.oceanbase.util.parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ConsistencyHint.java, v 0.1 2013-5-24 下午4:04:58 liangjie.li Exp $
 */
public class SQLHintParser {

    private static final String  consistencyHintPatternString = "/\\*\\+\\s*read_consistency\\((.+)\\)\\s*\\*/";
    private static final String  clusterHintPatternString     = "/\\*\\+\\s*read_cluster\\((.+)\\)\\s*\\*/";

    private static final String  MASTER                       = "master";
    private static final String  SLAVE                        = "slave";
    private static final String  STRONG                       = "strong";
    private static final String  WEAK                         = "weak";
    private static final String  STATIC                       = "static";
    private static final String  FROZEN                       = "frozen";

    private static final Pattern consistencyHintPattern       = Pattern.compile(
                                                                  consistencyHintPatternString,
                                                                  Pattern.CASE_INSENSITIVE);
    private static final Pattern clusterHintPattern           = Pattern.compile(
                                                                  clusterHintPatternString,
                                                                  Pattern.CASE_INSENSITIVE);

    /**
     * 
     * 
     * @param sql
     * @return
     */
    public static SqlHintType getConsistencyHint(String sql) {
        Matcher match = consistencyHintPattern.matcher(sql);
        if (match.find()) {
            String key = match.group(1);
            if (key.equalsIgnoreCase(STRONG)) {
                return SqlHintType.CONSISTENCY_STRONG;
            } else if (key.equalsIgnoreCase(WEAK)) {
                return SqlHintType.CONSISTENCY_WEAK;
            } else if (key.equalsIgnoreCase(FROZEN)) {
                return SqlHintType.CONSISTENCY_WEAK;
            } else if (key.equalsIgnoreCase(STATIC)) {
                return SqlHintType.CONSISTENCY_WEAK;
            } else {
                throw new IllegalArgumentException(
                    String
                        .format(
                            "consistency hint syntax error, only frozen, static, strong or weak support, your hint: [%s], SQL: [%s]",
                            key, sql));
            }
        }
        return SqlHintType.CONSISTENCY_NONE;
    }

    /**
     * 
     * 
     * @param sql
     * @return
     */
    public static SqlHintType getCluster(String sql) {
        Matcher match = clusterHintPattern.matcher(sql);
        if (match.find()) {
            String key = match.group(1);
            if (key.equalsIgnoreCase(MASTER)) {
                return SqlHintType.CLUSTER_MASTER;
            } else if (key.equalsIgnoreCase(SLAVE)) {
                return SqlHintType.CLUSTER_SLAVE;
            } else {
                throw new IllegalArgumentException(
                    String
                        .format(
                            "cluster hint syntax error, only master or slave support, your hint: [%s], SQL: [%s]",
                            key, sql));
            }
        }
        return SqlHintType.CLUSTER_NONE;
    }

    private SQLHintParser() {
    }

}