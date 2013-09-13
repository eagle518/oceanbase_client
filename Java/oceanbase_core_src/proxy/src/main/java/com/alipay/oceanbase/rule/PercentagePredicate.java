package com.alipay.oceanbase.rule;

import java.util.Random;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: PercentagePredicate.java, v 0.1 2013-5-24 下午12:54:57 liangjie.li Exp $
 */
public class PercentagePredicate implements Predicate {

    private final int percent;

    public PercentagePredicate(int percent) {
        this.percent = percent;
    }

    /**
     * 
     * @see com.alipay.oceanbase.rule.Predicate#needUpdate()
     */
    @Override
    public boolean needUpdate() {
        return percent >= new Random().nextInt(100) + 1;
    }

}