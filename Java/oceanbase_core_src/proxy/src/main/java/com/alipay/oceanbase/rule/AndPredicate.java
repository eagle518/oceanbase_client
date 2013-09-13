package com.alipay.oceanbase.rule;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: AndPredicate.java, v 0.1 2013-5-24 下午12:54:36 liangjie.li Exp $
 */
public class AndPredicate implements Predicate {

    private final Predicate A;
    private final Predicate B;

    public AndPredicate(Predicate A, Predicate B) {
        this.A = A;
        this.B = B;
    }

    @Override
    public boolean needUpdate() {
        return A.needUpdate() && B.needUpdate();
    }
}