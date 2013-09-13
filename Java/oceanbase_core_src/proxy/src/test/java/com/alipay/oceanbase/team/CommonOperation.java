/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2013 All Rights Reserved.
 */
package com.alipay.oceanbase.team;

import java.util.Map;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 
 * @author liangjie.li@alipay.com
 * @version $Id: CommonOperation.java, v 0.1 2013-3-28 下午2:01:07 liangjie.li Exp $
 */
public final class CommonOperation {

    public static void testSelectCluster(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        Map<String, Object> map = (Map<String, Object>) jdbcTemplate
            .queryForMap("select * from __all_cluster");

        Assert.assertEquals(10, map.size());

        Assert.assertEquals(1L, map.get("cluster_id"));
        Assert.assertEquals("10.232.23.13", map.get("cluster_vip"));
        Assert.assertEquals(48948L, map.get("cluster_port"));
        Assert.assertEquals(1L, map.get("cluster_role"));
        Assert.assertNull(null, map.get("cluster_name"));
        Assert.assertNull(null, map.get("cluster_info"));
        Assert.assertEquals(100L, map.get("cluster_flow_percent"));
        Assert.assertEquals(0L, map.get("read_strategy"));
    }

    private CommonOperation() {

    }

}
