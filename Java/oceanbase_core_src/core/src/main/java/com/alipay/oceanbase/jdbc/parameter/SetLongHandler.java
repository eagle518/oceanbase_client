package com.alipay.oceanbase.jdbc.parameter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetLongHandler.java, v 0.1 Jun 7, 2013 7:18:42 PM liangjie.li Exp $
 */
public class SetLongHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setLong((Integer) args[0], (Long) args[1]);
    }
}
