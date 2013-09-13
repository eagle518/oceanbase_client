package com.alipay.oceanbase.jdbc.parameter;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetURLHandler.java, v 0.1 Jun 7, 2013 7:16:55 PM liangjie.li Exp $
 */
public class SetURLHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setURL((Integer) args[0], (URL) args[1]);
    }
}
