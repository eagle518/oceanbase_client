package com.alipay.oceanbase.jdbc.parameter;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetArrayHandler.java, v 0.1 Jun 7, 2013 7:20:10 PM liangjie.li Exp $
 */
public class SetArrayHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setArray((Integer) args[0], (Array) args[1]);
    }
}
