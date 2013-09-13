package com.alipay.oceanbase.jdbc.parameter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetNull2Handler.java, v 0.1 Jun 7, 2013 7:18:26 PM liangjie.li Exp $
 */
public class SetNull2Handler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setNull((Integer) args[0], (Integer) args[1], (String) args[2]);
    }
}
