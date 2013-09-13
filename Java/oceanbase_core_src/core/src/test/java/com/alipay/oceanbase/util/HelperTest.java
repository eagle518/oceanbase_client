package com.alipay.oceanbase.util;

import static com.alipay.oceanbase.util.Helper.atomicDecIfPositive;
import static com.alipay.oceanbase.util.Helper.buildReadDistTable;
import static com.alipay.oceanbase.util.Helper.getMySqlConURL;
import static com.alipay.oceanbase.util.Helper.getVersionReportSQL;
import static com.alipay.oceanbase.util.OBDataSourceConstants.READ_DIST_TABLE_SIZE;
import static junit.framework.Assert.assertEquals;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import com.alipay.oceanbase.config.ClusterConfig;
import com.alipay.oceanbase.exception.InvalidException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: HelperTest.java, v 0.1 Jun 19, 2013 10:54:43 AM liangjie.li Exp $
 */
public class HelperTest {

    @Test
    public void testAtomicDecIfPositive() {
        AtomicInteger ai = new AtomicInteger(5);

        assertEquals(4, atomicDecIfPositive(ai));
        assertEquals(4, ai.get());

        assertEquals(3, atomicDecIfPositive(ai));
        assertEquals(3, ai.get());

        assertEquals(2, atomicDecIfPositive(ai));
        assertEquals(2, ai.get());

        assertEquals(1, atomicDecIfPositive(ai));
        assertEquals(1, ai.get());

        assertEquals(0, atomicDecIfPositive(ai));
        assertEquals(0, ai.get());

        assertEquals(-1, atomicDecIfPositive(ai));
        assertEquals(0, ai.get());

        assertEquals(-1, atomicDecIfPositive(ai));
        assertEquals(0, ai.get());

        assertEquals(-1, atomicDecIfPositive(ai));
        assertEquals(0, ai.get());
    }

    @Test
    public void testGetMySqlConURL1() {
        assertEquals("jdbc:mysql://" + mockIp1 + ":" + mockPort1 + "/Java",
            getMySqlConURL(mockIp1, mockPort1));
    }

    @Test(expected = InvalidException.class)
    public void testGetMySqlConURL2() {
        assertEquals("jdbc:mysql://" + "" + ":" + mockPort1 + "/Java",
            getMySqlConURL("", mockPort1));
    }

    @Test(expected = InvalidException.class)
    public void testGetMySqlConURL3() {
        assertEquals("jdbc:mysql://" + mockIp1 + ":" + 0L + "/Java", getMySqlConURL(mockIp2, 0L));
    }

    @Test
    public void testGetVersionReportSQL() {
        assertEquals(
            "select /*+ client(obdatasource) client_version(1.1.0) mysql_driver(5.1) */ 'client_version'",
            getVersionReportSQL());
    }

    @Test
    public void testBuildReadDistTable1() {
        ClusterConfig[] result = buildReadDistTable(mockClusterConfigs1);

        int count1 = 0, count2 = 0;
        for (ClusterConfig cc : result) {

            if (cc.equals(mockData1)) {
                count1++;
            } else if (cc.equals(mockData2)) {
                count2++;
            }
        }

        assertEquals(mockPercent1, count1);
        assertEquals(mockPercent2, count2);
    }

    @Test
    public void testBuildReadDistTable2() {
        ClusterConfig[] result = buildReadDistTable(mockClusterConfigs2);

        int count1 = 0, count2 = 0;
        for (ClusterConfig cc : result) {

            if (cc.equals(mockData3)) {
                count1++;
            } else if (cc.equals(mockData4)) {
                count2++;
            }
        }

        assertEquals(mockPercent3 * READ_DIST_TABLE_SIZE / (mockPercent3 + mockPercent4), count1);
        assertEquals(mockPercent4 * READ_DIST_TABLE_SIZE / (mockPercent3 + mockPercent4), count2);
    }

    String mockIp1, mockIp2;
    long   mockPort1, mockPort2, mockClusterId1, mockClusterId2, mockRole1, mockRole2,
            mockPercent1, mockPercent2, mockPercent3, mockPercent4, mockReadStrategy1,
            mockReadStrategy2;
    ClusterConfig mockData1, mockData2, mockData3, mockData4;
    Set<ClusterConfig> mockClusterConfigs1, mockClusterConfigs2;

    @Before
    public void setUP() {
        mockIp1 = "10.1.1.1";
        mockIp2 = "10.1.1.2";
        mockPort1 = 2828L;
        mockPort2 = 2929L;
        mockClusterId1 = 1L;
        mockClusterId2 = 2L;
        mockRole1 = 1L;
        mockRole2 = 2L;
        mockPercent1 = 40L;
        mockPercent2 = 60L;
        mockPercent3 = 150L;
        mockPercent4 = 100L;
        mockReadStrategy1 = 0L;
        mockReadStrategy2 = 1L;

        mockClusterConfigs1 = new LinkedHashSet<ClusterConfig>();
        mockData1 = new ClusterConfig(mockIp1, mockPort1, mockClusterId1, mockRole1, mockPercent1,
            mockReadStrategy1);
        mockData2 = new ClusterConfig(mockIp2, mockPort2, mockClusterId2, mockRole2, mockPercent2,
            mockReadStrategy2);
        mockClusterConfigs1.add(mockData1);
        mockClusterConfigs1.add(mockData2);

        mockClusterConfigs2 = new LinkedHashSet<ClusterConfig>();
        mockData3 = new ClusterConfig(mockIp1, mockPort1, mockClusterId1, mockRole1, mockPercent3,
            mockReadStrategy1);
        mockData4 = new ClusterConfig(mockIp2, mockPort2, mockClusterId2, mockRole2, mockPercent4,
            mockReadStrategy2);
        mockClusterConfigs2.add(mockData3);
        mockClusterConfigs2.add(mockData4);

    }
}
