package com.alipay.oceanbase.rule;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: BooleanPredicate.java, v 0.1 2013-5-24 下午12:54:41 liangjie.li Exp $
 */
public class BooleanPredicate implements Predicate {

    private final boolean confirm;

    public BooleanPredicate(boolean confirm) {
        this.confirm = confirm;
    }

    @Override
    public boolean needUpdate() {
        return this.confirm;
    }

}