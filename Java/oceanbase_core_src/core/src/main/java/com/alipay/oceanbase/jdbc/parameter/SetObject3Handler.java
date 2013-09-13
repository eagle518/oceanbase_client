package com.alipay.oceanbase.jdbc.parameter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetObject3Handler.java, v 0.1 Jun 7, 2013 7:18:06 PM liangjie.li Exp $
 */
public class SetObject3Handler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setObject((Integer) args[0], args[1], (Integer) args[2], (Integer) args[3]);
    }
}
