package com.alipay.oceanbase.rule;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: Predicate.java, v 0.1 2013-5-24 下午12:55:02 liangjie.li Exp $
 */
public interface Predicate {

    /**
     * 
     * 
     * @return
     */
    public abstract boolean needUpdate();
}
