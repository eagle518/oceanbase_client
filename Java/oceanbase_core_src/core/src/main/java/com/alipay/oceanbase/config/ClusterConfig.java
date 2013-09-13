package com.alipay.oceanbase.config;

import static com.alipay.oceanbase.util.OBDataSourceConstants.MASTER;

import java.io.Serializable;
import java.util.Set;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.alipay.oceanbase.strategy.EquityStrategy;
import com.alipay.oceanbase.strategy.WeakConsistencyStrategy;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ClusterConfig.java, v 0.1 Jun 6, 2013 3:47:55 PM liangjie.li Exp $
 */
public class ClusterConfig implements Serializable {
    /**  */
    private static final long       serialVersionUID = 8012239513088420157L;

    // cluster info
    private long                    role             = 0;
    private long                    percent          = 0;
    private long                    port             = 0;
    private long                    clusterid        = 0;

    private String                  ip               = null;
    private Set<MergeServerConfig>  servers          = null;                                   // cluster merge servers
    private EquityStrategy          equityStrategy   = null;                                   // the weak consistency reading strategy of the cluster 

    private WeakConsistencyStrategy readStrategy     = WeakConsistencyStrategy.RANDOM_STRATEGY; // default strategy

    public ClusterConfig(String ip, long port, long clusterId, long role, long percent) {
        this.ip = ip;
        this.port = port;
        this.clusterid = clusterId;
        this.role = role;
        this.percent = percent;
    }

    public ClusterConfig(String ip, long port, long clusterId, long role, long percent,
                         long readStrategy) {
        this(ip, port, clusterId, role, percent);
        this.readStrategy = WeakConsistencyStrategy.getStrategy(readStrategy);
    }

    /**
     * 
     * 
     * @return
     */
    public boolean isInvalid() {
        return equityStrategy.isInvalid();
    }

    /**
     * 
     * 
     * @return
     */
    public boolean isMaster() {
        if (this.role == MASTER) {
            return true;
        }
        return false;
    }

    /**
     * 
     * 
     * @param cc
     * @return
     */
    public boolean isPercentChange(ClusterConfig cc) {
        if (cc != null) {
            return cc.getPercent() != this.percent;
        }

        return false;
    }

    /**
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "isMaster:" + ((role == MASTER) ? "true" : "false") + ", percent:" + percent
               + ", mergeservers: " + servers.toString();

    }

    /**
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object r) {
        if (this == r) {
            return true;
        }

        if (r instanceof ClusterConfig) {
            ClusterConfig cc = (ClusterConfig) r;
            if ((this.hashCode() == cc.hashCode()) && this.port == cc.port && this.ip.equals(cc.ip)
                && this.getReadStrategy() == cc.getReadStrategy() && this.role == cc.role) {

                return true;
            }
        }
        return false;
    }

    /**
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.ip).append(this.port).append(this.readStrategy)
            .append(this.role).toHashCode();
    }

    // ///////////////////////// setter and getter /////////////////////////

    public Set<MergeServerConfig> getServers() {
        return servers;
    }

    public void setServers(Set<MergeServerConfig> servers) {
        this.servers = servers;
    }

    public long getClusterid() {
        return clusterid;
    }

    public long getPercent() {
        return percent;
    }

    public void setPercent(long percent) {
        this.percent = percent;
    }

    public long getRole() {
        return role;
    }

    public long getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public WeakConsistencyStrategy getReadStrategy() {
        return readStrategy;
    }

    public EquityStrategy getEquityStrategy() {
        return equityStrategy;
    }

    public void setEquityStrategy(EquityStrategy equityStrategy) {
        this.equityStrategy = equityStrategy;
    }

}
