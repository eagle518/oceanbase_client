package com.alipay.oceanbase.util.parse;

import static com.alipay.oceanbase.util.parse.SQLParser.getSqlType;
import static junit.framework.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: SQLParserTest.java, v 0.1 Jun 18, 2013 12:23:50 PM liangjie.li Exp $
 */
public class SQLParserTest {

    String mockSQL1, mockSQL2, mockSQL3, mockSQL4, mockSQL5, mockSQL6, mockSQL7, mockSQL8,
            mockSQL9, mockSQL10, mockSQL11, mockSQL12, mockSQL13, mockSQL14, mockSQL15, mockSQL16, mockSQL17, mockSQL18, mockSQL19;

    @Test
    public void testGetSqlType0() throws SQLException {
        assertEquals(SqlType.SELECT, getSqlType(mockSQL1));
        assertEquals(SqlType.SELECT_FOR_UPDATE, getSqlType(mockSQL2));
        assertEquals(SqlType.UPDATE, getSqlType(mockSQL3));
        assertEquals(SqlType.REPLACE, getSqlType(mockSQL4));
        assertEquals(SqlType.DROP, getSqlType(mockSQL5));
        assertEquals(SqlType.DELETE, getSqlType(mockSQL6));
        assertEquals(SqlType.INSERT, getSqlType(mockSQL7));
        assertEquals(SqlType.EXPLAIN, getSqlType(mockSQL8));
        assertEquals(SqlType.EXPLAIN, getSqlType(mockSQL9));
        assertEquals(SqlType.TRUNCATE, getSqlType(mockSQL10));
        assertEquals(SqlType.CREATE, getSqlType(mockSQL11));
        assertEquals(SqlType.ALTER, getSqlType(mockSQL12));
        assertEquals(SqlType.SHOW, getSqlType(mockSQL13));

    } 
    
    @Test(expected = SQLException.class)  
    public void testGetSqlType1() throws SQLException {
        assertEquals(SqlType.SELECT, getSqlType(mockSQL14));
    }
    
    @Test(expected = SQLException.class)  
    public void testGetSqlType2() throws SQLException {
        assertEquals(SqlType.SELECT_FOR_UPDATE, getSqlType(mockSQL15));
    }
    
    @Test(expected = SQLException.class)  
    public void testGetSqlType3() throws SQLException {
        assertEquals(SqlType.UPDATE, getSqlType(mockSQL16));
    }
    
    @Test(expected = SQLException.class)  
    public void testGetSqlType4() throws SQLException {
        assertEquals(SqlType.REPLACE, getSqlType(mockSQL17));
    }
    
    @Test(expected = SQLException.class)  
    public void testGetSqlType5() throws SQLException {
        assertEquals(SqlType.INSERT, getSqlType(mockSQL18));
    }
    
    @Test(expected = SQLException.class)  
    public void testGetSqlType6() throws SQLException {
        assertEquals(SqlType.DELETE, getSqlType(mockSQL19));
    }
    
    @Before
    public void setUp() {
        mockSQL1 = " select col1, col2 from tab1 where col1 = 1 order by col3 desc limit 1";
        mockSQL2 = "select col1, col2 from tab1 where col1 = 1 for update ";
        mockSQL3 = " update tab1 set col1 = 'a' where col2 = 1";
        mockSQL4 = " replace into tab1(col1, col2, col3) values(1, 'a', 'b') ";
        mockSQL5 = "drop table tab1";
        mockSQL6 = "delete from tab1 where col1 = 1";
        mockSQL7 = "insert into tab1(col1, col2, col3) values (1, 'a', 'b')";
        mockSQL8 = " explain select col1, col2 from tab1 where col1 = 1 order by col3 desc limit 1";
        mockSQL9 = " explain  update tab1 set col1 = 'a' where col2 = 1";
        mockSQL10 = " truncate table tab";
        mockSQL11 = " create table tab1 (col1 int1 primary key, col2 varchar(128))";
        mockSQL12 = "alter table tab1 col2 varchar(256)";
        mockSQL13 = " show variables like 'ob_read_consistency'";
       
        mockSQL14 = " selec col1, col2 from tab1 where col1 = 1 order by col3 desc limit 1";
        mockSQL15= "se lect col1, col2 from tab1 where col1 = 1 for update ";
        mockSQL16 = " u pdate tab1 set col1 = 'a' where col2 = 1";
        mockSQL17 = " relace into tab1(col1, col2, col3) values(1, 'a', 'b') ";
        mockSQL18 = "in sert into tab1(col1, col2, col3) values (1, 'a', 'b')";
        mockSQL19 = "deete from tab1 where col1 = 1";

    }
}
