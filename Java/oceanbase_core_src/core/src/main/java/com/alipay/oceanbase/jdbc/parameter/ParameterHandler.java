package com.alipay.oceanbase.jdbc.parameter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ParameterHandler.java, v 0.1 Jun 7, 2013 7:20:40 PM liangjie.li Exp $
 */
public interface ParameterHandler {

    /**
     * 
     * 
     * @param stmt
     * @param args
     * @throws SQLException
     */
    void setParameter(PreparedStatement stmt, Object[] args) throws SQLException;
}
