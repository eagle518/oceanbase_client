package com.alipay.oceanbase.config;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.alipay.oceanbase.strategy.EquityStrategy;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ClusterConfigTest.java, v 0.1 Jun 18, 2013 11:56:25 AM liangjie.li Exp $
 */
public class ClusterConfigTest {

    String mockIp1, mockIp2;
    long   mockPort1, mockPort2, clusterId1, clusterId2, role1, role2, percent1, percent2,
            readStrategy1, readStrategy2;

    ClusterConfig mockData1, mockData2, mockData3, mockData4, mockData5, mockData6, mockData7,
            mockData8, mockData9;

    @Test
    public void testIsMaster() {
        assertTrue(mockData1.isMaster());
        assertTrue(mockData2.isMaster());
        assertFalse(mockData3.isMaster());
    }

    @Test
    public void testEquals() {
        assertTrue(mockData1.equals(mockData1));
        assertTrue(mockData1.equals(mockData2));

        assertFalse(mockData1.equals(mockData3));
        assertFalse(mockData1.equals(mockData4));
        assertFalse(mockData1.equals(mockData5));
        assertTrue(mockData1.equals(mockData6));
        assertFalse(mockData1.equals(mockData7));
        assertTrue(mockData1.equals(mockData8));
        assertFalse(mockData1.equals(mockData9));

        assertTrue(mockData1.hashCode() == mockData2.hashCode());
    }

    @Test
    public void testIsPercentChange() {
        assertFalse(mockData1.isPercentChange(mockData2));
        assertTrue(mockData1.isPercentChange(mockData3));
        assertFalse(mockData1.isPercentChange(mockData7));
        assertTrue(mockData1.isPercentChange(mockData8));
    }

    @Test
    public void testIsInvalid() {
        EquityStrategy es = mock(EquityStrategy.class);
        when(es.isInvalid()).thenReturn(true);

        mockData1.setEquityStrategy(es);
        assertTrue(mockData1.isInvalid());

        when(es.isInvalid()).thenReturn(false);
        assertFalse(mockData1.isInvalid());
    }

    @Before
    public void setUp() {
        mockIp1 = "10.1.1.1";
        mockIp2 = "10.1.1.2";
        mockPort1 = 2828L;
        mockPort2 = 2929L;
        clusterId1 = 1L;
        clusterId2 = 2L;
        role1 = 1L;
        role2 = 2L;
        percent1 = 40L;
        percent2 = 60L;
        readStrategy1 = 0L;
        readStrategy2 = 1L;

        mockData1 = new ClusterConfig(mockIp1, mockPort1, clusterId1, role1, percent1,
            readStrategy1);
        mockData2 = new ClusterConfig(mockIp1, mockPort1, clusterId1, role1, percent1,
            readStrategy1);
        mockData3 = new ClusterConfig(mockIp2, mockPort2, clusterId2, role2, percent2,
            readStrategy2);
        mockData4 = new ClusterConfig(mockIp2, mockPort1, clusterId1, role1, percent1,
            readStrategy1);
        mockData5 = new ClusterConfig(mockIp1, mockPort2, clusterId1, role1, percent1,
            readStrategy1);
        mockData6 = new ClusterConfig(mockIp1, mockPort1, clusterId2, role1, percent1,
            readStrategy1);
        mockData7 = new ClusterConfig(mockIp1, mockPort1, clusterId1, role2, percent1,
            readStrategy1);
        mockData8 = new ClusterConfig(mockIp1, mockPort1, clusterId1, role1, percent2,
            readStrategy1);
        mockData9 = new ClusterConfig(mockIp1, mockPort1, clusterId1, role1, percent1,
            readStrategy2);
    }
}
