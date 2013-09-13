package com.alipay.oceanbase.rule;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OrPredicate.java, v 0.1 2013-5-24 下午12:54:52 liangjie.li Exp $
 */
public class OrPredicate implements Predicate {

    private final Predicate A;
    private final Predicate B;

    public OrPredicate(Predicate A, Predicate B) {
        this.A = A;
        this.B = B;
    }

    @Override
    public boolean needUpdate() {
        return A.needUpdate() || B.needUpdate();
    }
}