/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package com.alipay.oceanbase.jdbc.sorter;

import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: OceanbaseBaseExceptionSorter.java, v 0.1 2013-8-22 下午2:57:24 liangjie.li Exp $
 */
public class OceanbaseBaseExceptionSorter {

    public static boolean isExceptionFatal(SQLException e) {
        return _isExceptionFatal(e, true);
    }

    public static boolean isNotMasterClusterFatal(SQLException e) {
        return _isExceptionFatal(e, false);
    }

    protected static boolean _isExceptionFatal(SQLException e, boolean isType) {
        int loopCount = 20;

        Throwable cause = e;
        while (cause != null) {
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;

                if (isType && isExceptionFatal0(sqlException)) {
                    return true;
                } else if (!isType && isNotMasterClusterFatal0(sqlException)) {
                    return true;
                }
            }
            cause = cause.getCause();
            if (--loopCount < 0) {
                break;
            }
        }
        return false;
    }

    private static boolean isNotMasterClusterFatal0(SQLException e) {
        final int errorCode = e.getErrorCode();

        switch (errorCode) {
            case -38:
                return true;
        }

        return false;
    }

    private static boolean isExceptionFatal0(SQLException e) {
        String sqlState = e.getSQLState();
        final int errorCode = Math.abs(e.getErrorCode());

        if (sqlState != null && sqlState.startsWith("08")) {
            return true;
        }

        if (StringUtils.isNotBlank(e.getMessage())) {
            final String errorText = e.getMessage().toUpperCase();

            if (errorCode == 0
                && (errorText.indexOf("COMMUNICATIONS LINK FAILURE") > -1 || errorText
                    .indexOf("COULD NOT CREATE CONNECTION") > -1)
                || errorText.indexOf("NO DATASOURCE") > -1
                || errorText.indexOf("NO ALIVE DATASOURCE") > -1) {
                return true;
            }
        }
        return false;
    }
}