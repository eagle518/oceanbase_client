package com.alipay.oceanbase.team;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * 
 * @author liangjie.li
 * @version $Id: RunAllJDBCTestsCase.java, v 0.1 2012-8-14 ����12:40:28 liangjie.li Exp $
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ BASIC1000.class, BASIC2000.class })
public class RunAllTeamTestsCase {
}
