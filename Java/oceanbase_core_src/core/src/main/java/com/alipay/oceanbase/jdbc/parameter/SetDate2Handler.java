package com.alipay.oceanbase.jdbc.parameter;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Calendar;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetDate2Handler.java, v 0.1 Jun 7, 2013 7:19:04 PM liangjie.li Exp $
 */
public class SetDate2Handler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setDate((Integer) args[0], (Date) args[1], (Calendar) args[2]);
    }
}
