package com.alipay.oceanbase.jdbc.parameter;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetCharacterStreamHandler.java, v 0.1 Jun 7, 2013 7:19:22 PM liangjie.li Exp $
 */
public class SetCharacterStreamHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setCharacterStream((Integer) args[0], (Reader) args[1], (Integer) args[2]);
    }
}
