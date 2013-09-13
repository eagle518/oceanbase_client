package com.alipay.oceanbase.jdbc.parameter;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetClobHandler.java, v 0.1 Jun 7, 2013 7:19:15 PM liangjie.li Exp $
 */
public class SetClobHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setClob((Integer) args[0], (Clob) args[1]);
    }
}
