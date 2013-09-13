package com.alipay.oceanbase.config;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * mergeserver config info
 * 
 * 
 * @author liangjie.li
 * @version $Id: MergeServerConfig.java, v 0.1 Jun 6, 2013 3:37:22 PM liangjie.li Exp $
 */
public class MergeServerConfig implements Serializable {

    /**  */
    private static final long serialVersionUID = -5472022720974346890L;

    public MergeServerConfig(String ip, Long port) {
        if (StringUtils.isBlank(ip) || port.compareTo(0L) <= 0) {
            throw new IllegalArgumentException("mergeserver ip (port) must not be blank!");
        }
        this.ip = ip;
        this.port = port;
    }

    private String ip;
    private Long   port;

    /**
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return this.getIp() + ":" + this.getPort();
    }

    /**
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof MergeServerConfig) {
            MergeServerConfig msc = (MergeServerConfig) o;
            if (msc.toString().equals(this.toString())) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.ip).append(this.port).toHashCode();
    }

    // ///////////////////// setter and getter /////////////////////
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getPort() {
        return port;
    }

    public void setPort(Long port) {
        this.port = port;
    }

}
