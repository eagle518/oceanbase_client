package com.alipay.oceanbase.util.parse;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ConsistencyHintTest.java, v 0.1 Jun 18, 2013 11:59:23 AM liangjie.li Exp $
 */
public class SQLHintParserTest {

    String mockSQL1, mockSQL2, mockSQL3, mockSQL4, mockSQL5, mockSQL6, mockSQL7, mockSQL8,
            mockSQL9, mockSQL10, mockSQL11;

    @Test
    public void testGetConsistencyHint() {
        Assert.assertEquals(SqlHintType.CONSISTENCY_STRONG,
            SQLHintParser.getConsistencyHint(mockSQL1));
        Assert.assertEquals(SqlHintType.CONSISTENCY_WEAK,
            SQLHintParser.getConsistencyHint(mockSQL2));
        Assert.assertEquals(SqlHintType.CONSISTENCY_NONE,
            SQLHintParser.getConsistencyHint(mockSQL3));
        Assert.assertEquals(SqlHintType.CONSISTENCY_NONE,
            SQLHintParser.getConsistencyHint(mockSQL4));
        Assert.assertEquals(SqlHintType.CONSISTENCY_NONE,
            SQLHintParser.getConsistencyHint(mockSQL5));
    }

    @Test
    public void testGetCluster() {
        Assert.assertEquals(SqlHintType.CLUSTER_MASTER, SQLHintParser.getCluster(mockSQL6));
        Assert.assertEquals(SqlHintType.CLUSTER_SLAVE, SQLHintParser.getCluster(mockSQL7));
        Assert.assertEquals(SqlHintType.CLUSTER_NONE, SQLHintParser.getCluster(mockSQL8));
        Assert.assertEquals(SqlHintType.CLUSTER_NONE, SQLHintParser.getCluster(mockSQL9));
        Assert.assertEquals(SqlHintType.CLUSTER_NONE, SQLHintParser.getCluster(mockSQL10));
        Assert.assertEquals(SqlHintType.CLUSTER_NONE, SQLHintParser.getCluster(mockSQL11));
    }

    @Before
    public void setUp() {
        mockSQL1 = "select col1, /*+read_consistency(strong)*/ col2 from tab where col = 'a'";
        mockSQL2 = "select /*+read_consistency(weak)*/ * from tab where col = 'a'";
        mockSQL3 = "select /*+read_consistency()*/ * from tab where col = 'a'";
        mockSQL4 = "select /*+read_consistency(weak)+*/ * from tab where col = 'a'";
        mockSQL5 = "select * from tab where col = 'a'";

        mockSQL6 = "select col1, /*+read_cluster(master)*/ col2 from tab where col = 'a'";
        mockSQL7 = "select /*+read_cluster(slave)*/ * from tab where col = 'a'";
        mockSQL8 = "select /*+read_cluster()*/ * from tab where col = 'a'";
        mockSQL9 = "select /*+read_cluster(slave)+*/ * from tab where col = 'a'";
        mockSQL10 = "select * from tab where col = 'a'";
        mockSQL11 = "insert into tab(c1, c2) values(?,?)";
    }

}
