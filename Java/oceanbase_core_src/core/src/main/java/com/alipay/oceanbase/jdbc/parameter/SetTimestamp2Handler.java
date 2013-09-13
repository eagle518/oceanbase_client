package com.alipay.oceanbase.jdbc.parameter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetTimestamp2Handler.java, v 0.1 Jun 7, 2013 7:17:18 PM liangjie.li Exp $
 */
public class SetTimestamp2Handler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setTimestamp((Integer) args[0], (Timestamp) args[1], (Calendar) args[2]);
    }
}
