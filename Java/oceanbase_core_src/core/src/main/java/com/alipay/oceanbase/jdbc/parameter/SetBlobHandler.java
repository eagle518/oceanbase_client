package com.alipay.oceanbase.jdbc.parameter;

import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SetBlobHandler.java, v 0.1 Jun 7, 2013 7:19:45 PM liangjie.li Exp $
 */
public class SetBlobHandler implements ParameterHandler {

    /**
     * 
     * @see com.alipay.oceanbase.jdbc.parameter.ParameterHandler#setParameter(java.sql.PreparedStatement, java.lang.Object[])
     */
    public void setParameter(PreparedStatement stmt, Object[] args) throws SQLException {
        stmt.setBlob((Integer) args[0], (Blob) args[1]);
    }
}
