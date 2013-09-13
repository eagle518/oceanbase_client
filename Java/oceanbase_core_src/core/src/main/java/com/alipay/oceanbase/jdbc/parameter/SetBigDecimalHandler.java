package com.alipay.oceanbase.jdbc.parameter;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetBigDecimalHandler.java, v 0.1 Jun 7, 2013 7:19:59 PM liangjie.li Exp $
 */
public class SetBigDecimalHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setBigDecimal((Integer) args[0], (BigDecimal) args[1]);
    }
}
