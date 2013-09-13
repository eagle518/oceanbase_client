package com.alipay.oceanbase.strategy;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ReadMsStrategy.java, v 0.1 2013-5-24 下午3:52:18 liangjie.li Exp $
 */
public enum WeakConsistencyStrategy {

    RANDOM_STRATEGY(2), CONSISTENT_HASHING_STRATEGY(1), ROUNDROBIN_STRATEGY(0);

    private final long code;

    private WeakConsistencyStrategy(long code) {
        this.code = code;
    }

    /**
     * 
     * 
     * @param code
     * @return
     */
    public static WeakConsistencyStrategy getStrategy(long code) {
        if (code == RANDOM_STRATEGY.code) {
            return RANDOM_STRATEGY;
        } else if (code == CONSISTENT_HASHING_STRATEGY.code) {
            return CONSISTENT_HASHING_STRATEGY;
        } else if (code == ROUNDROBIN_STRATEGY.code) {
            return ROUNDROBIN_STRATEGY;
        } else {
            return ROUNDROBIN_STRATEGY;
        }
    }

}