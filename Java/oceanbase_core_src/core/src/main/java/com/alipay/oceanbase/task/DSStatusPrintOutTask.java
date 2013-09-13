package com.alipay.oceanbase.task;

import java.util.Set;

import com.alipay.oceanbase.OBGroupDataSource;
import com.alipay.oceanbase.config.ClusterConfig;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: DSStatusPrintOutTask.java, v 0.1 Jun 9, 2013 12:11:25 PM liangjie.li Exp $
 */
public class DSStatusPrintOutTask implements Runnable {

    private OBGroupDataSource obGroupDataSource;

    public DSStatusPrintOutTask(OBGroupDataSource obGroupDataSource) {
        this.obGroupDataSource = obGroupDataSource;
    }

    /**
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        Set<ClusterConfig> clusterConfigs = obGroupDataSource.getConfig().getClusterConfigs();

        for (ClusterConfig cc : clusterConfigs) {
            cc.getEquityStrategy().printDSStatus();
        }
    }

}
