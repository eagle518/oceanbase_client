package com.alipay.oceanbase;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.alipay.oceanbase.rule.BasePredicateTest;
import com.alipay.oceanbase.team.BASIC1000;
import com.alipay.oceanbase.team.BASIC2000;

/**
 * 
 * @author liangjie.li
 * @version $Id: RunAllJDBCTestsCase.java, v 0.1 2012-8-14 下午12:40:28 liangjie.li Exp $
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ BasePredicateTest.class, BASIC1000.class, BASIC2000.class })
public class AllTests {
}
