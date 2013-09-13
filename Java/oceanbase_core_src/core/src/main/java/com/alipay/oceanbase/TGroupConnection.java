package com.alipay.oceanbase;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alipay.oceanbase.exception.NotSupportedException;
import com.alipay.oceanbase.factory.DataSourceHolder;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: TGroupConnection.java, v 0.1 2013-5-24 下午4:13:53 liangjie.li Exp $
 */
public class TGroupConnection implements Connection {

    private static final Logger logger = Logger.getLogger(TGroupConnection.class);

    private OBGroupDataSource   tGroupDataSource;

    private String              username;
    private String              password;

    public TGroupConnection(OBGroupDataSource tGroupDataSource) {
        this.tGroupDataSource = tGroupDataSource;
    }

    public TGroupConnection(OBGroupDataSource tGroupDataSource, String username, String password) {
        this(tGroupDataSource);
        this.username = username;
        this.password = password;
    }

    private Connection              rBaseConnection = null;
    private Connection              wBaseConnection = null;

    private static DatabaseMetaData meta            = null;

    Connection getBaseConnection(String sql, boolean isRead) throws SQLException {
        if (isRead && isAutoCommit) {
            return rBaseConnection;
        } else {
            if (wBaseConnection != null) {
                return wBaseConnection;
            } else {
                return null;
            }
        }
    }

    Connection createNewConnection(DataSourceHolder dsw, boolean isRead) throws SQLException {
        Connection conn;
        if (username != null)
            conn = dsw.getDataSource().getConnection(username, password);
        else
            conn = dsw.getDataSource().getConnection();

        setBaseConnection(conn, dsw, isRead && isAutoCommit);

        if (!isRead || !isAutoCommit)
            conn.setAutoCommit(isAutoCommit);
        return conn;
    }

    private void setBaseConnection(Connection baseConnection, DataSourceHolder dsw, boolean isRead) {
        if (baseConnection == null) {
            logger.warn("setBaseConnection to null !!");
        } else {
            try {
                TGroupConnection.meta = baseConnection.getMetaData();
            } catch (SQLException ex) {
                logger.warn("get DatabaseMetaData fail");
            }
        }

        if (isRead)
            closeReadConnection();
        else
            closeWriteConnection();

        if (isRead) {
            rBaseConnection = baseConnection;
        } else {
            wBaseConnection = baseConnection;
        }
    }

    private void closeReadConnection() {

        if (rBaseConnection != null && rBaseConnection != wBaseConnection) {
            try {
                rBaseConnection.close();
            } catch (SQLException e) {
                logger.error("close rBaseConnection failed.", e);
            }
            rBaseConnection = null;
        }
    }

    private void closeWriteConnection() {

        if (wBaseConnection != null && rBaseConnection != wBaseConnection) {
            try {
                wBaseConnection.close();
            } catch (SQLException e) {
                logger.error("close wBaseConnection failed.", e);
            }
            wBaseConnection = null;
        }
    }

    private Set<TGroupStatement> openedStatements = new HashSet<TGroupStatement>(2);

    void removeOpenedStatements(Statement statement) {
        if (!openedStatements.remove(statement)) {
            logger.warn("current statmenet" + statement + " doesn't exist!");
        }
    }

    private boolean closed;

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public void close() throws SQLException {
        if (closed) {
            return;
        }
        closed = true;

        List<SQLException> exceptions = new LinkedList<SQLException>();
        try {

            for (TGroupStatement stmt : openedStatements) {
                try {
                    stmt.close(false);
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }

            try {
                if (rBaseConnection != null && !rBaseConnection.isClosed()) {
                    rBaseConnection.close();
                }
            } catch (SQLException e) {
                exceptions.add(e);
            }
            try {
                if (wBaseConnection != null && !wBaseConnection.isClosed()) {
                    wBaseConnection.close();
                }
            } catch (SQLException e) {
                exceptions.add(e);
            }
        } finally {
            openedStatements.clear();
            rBaseConnection = null;
            wBaseConnection = null;
        }
    }

    public TGroupStatement createStatement() throws SQLException {
        checkClosed();
        TGroupStatement stmt = new TGroupStatement(tGroupDataSource, this);
        openedStatements.add(stmt);
        return stmt;
    }

    public TGroupStatement createStatement(int resultSetType, int resultSetConcurrency)
                                                                                       throws SQLException {
        TGroupStatement stmt = createStatement();
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    public TGroupStatement createStatement(int resultSetType, int resultSetConcurrency,
                                           int resultSetHoldability) throws SQLException {
        TGroupStatement stmt = createStatement(resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    public TGroupPreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        TGroupPreparedStatement stmt = new TGroupPreparedStatement(tGroupDataSource, this, sql);
        openedStatements.add(stmt);
        return stmt;
    }

    public TGroupPreparedStatement prepareStatement(String sql, int resultSetType,
                                                    int resultSetConcurrency) throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    public TGroupPreparedStatement prepareStatement(String sql, int resultSetType,
                                                    int resultSetConcurrency,
                                                    int resultSetHoldability) throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql, resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    public TGroupPreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
                                                                                      throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setAutoGeneratedKeys(autoGeneratedKeys);
        return stmt;
    }

    public TGroupPreparedStatement prepareStatement(String sql, int[] columnIndexes)
                                                                                    throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnIndexes(columnIndexes);
        return stmt;
    }

    public TGroupPreparedStatement prepareStatement(String sql, String[] columnNames)
                                                                                     throws SQLException {
        TGroupPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnNames(columnNames);
        return stmt;
    }

    private boolean isAutoCommit = true;

    public void setAutoCommit(boolean autoCommit0) throws SQLException {
        if (logger.isDebugEnabled()) {
            logger.debug("set auto commit:" + autoCommit0);
        }

        checkClosed();
        if (this.isAutoCommit == autoCommit0) {
            return;
        }
        this.isAutoCommit = autoCommit0;

        if (this.wBaseConnection != null) {
            this.wBaseConnection.setAutoCommit(autoCommit0);
        }
    }

    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return isAutoCommit;
    }

    public void commit() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }
        if (wBaseConnection != null) {
            try {
                wBaseConnection.commit();
            } catch (SQLException e) {
                throw e;
            }
        }
    }

    public void rollback() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }

        if (wBaseConnection != null) {
            try {
                wBaseConnection.rollback();
            } catch (SQLException e) {
                throw e;
            }
        }
    }

    private int transactionIsolation = -1;

    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    public void setTransactionIsolation(int transactionIsolation) throws SQLException {
        checkClosed();
        this.transactionIsolation = transactionIsolation;
    }

    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        if (rBaseConnection != null)
            return rBaseConnection.getWarnings();
        else if (wBaseConnection != null)
            return wBaseConnection.getWarnings();
        else
            return null;
    }

    public void clearWarnings() throws SQLException {
        checkClosed();
        if (rBaseConnection != null)
            rBaseConnection.clearWarnings();
        if (wBaseConnection != null)
            wBaseConnection.clearWarnings();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return TGroupConnection.meta;
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback");
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint");
    }

    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("getCatalog");
    }

    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("setCatalog");
    }

    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("setHoldability");
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("getTypeMap");
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("setTypeMap");
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("nativeSQL");
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public Clob createClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLException("not support exception");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("not support exception");
    }

    public boolean isValid(int timeout) throws SQLException {
        throw new SQLException("not support exception");
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new NotSupportedException("not support exception");
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new NotSupportedException("not support exception");
    }

    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("not support exception");
    }

    public Properties getClientInfo() throws SQLException {
        throw new SQLException("not support exception");
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("not support exception");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("not support exception");
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                 throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return null;
    }

}
