package com.alipay.oceanbase.strategy;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: WeakConsistencyStrategyTest.java, v 0.1 Jun 19, 2013 10:27:44 AM liangjie.li Exp $
 */
public class WeakConsistencyStrategyTest {

    @Test
    public void testGetStrategy0() {
        assertEquals(WeakConsistencyStrategy.RANDOM_STRATEGY,
            WeakConsistencyStrategy.getStrategy(0L));
    }

    @Test
    public void testGetStrategy1() {
        assertEquals(WeakConsistencyStrategy.CONSISTENT_HASHING_STRATEGY,
            WeakConsistencyStrategy.getStrategy(1L));
    }

    @Test
    public void testGetStrategy2() {
        assertEquals(WeakConsistencyStrategy.RANDOM_STRATEGY,
            WeakConsistencyStrategy.getStrategy(100L));
    }

}
