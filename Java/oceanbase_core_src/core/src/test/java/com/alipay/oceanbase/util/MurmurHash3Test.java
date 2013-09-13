package com.alipay.oceanbase.util;

import static com.alipay.oceanbase.util.OBDataSourceConstants.MURMURHASH_M;
import static junit.framework.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: MurmurHash3Test.java, v 0.1 Jun 19, 2013 10:33:01 AM liangjie.li Exp $
 */
@RunWith(PowerMockRunner.class)
public class MurmurHash3Test {

    String mockString;

    @Test
    @PrepareForTest(MurmurHash3.class)
    public void testMurmurhash() throws Exception {
        spy(MurmurHash3.class);
        doReturn(1).when(MurmurHash3.class, "murmurhash3_x86_32", mockString.getBytes(), 0,
            mockString.getBytes().length, MURMURHASH_M);

        assertEquals(1, MurmurHash3.murmurhash(mockString));
    }

    @Test
    public void testMurmurhash3_x86_32() {
        assertEquals(1883996636, MurmurHash3.murmurhash3_x86_32(mockString.getBytes(), 0,
            mockString.length(), MURMURHASH_M));
    }

    @Before
    public void setUp() {
        mockString = "test";
    }

}
