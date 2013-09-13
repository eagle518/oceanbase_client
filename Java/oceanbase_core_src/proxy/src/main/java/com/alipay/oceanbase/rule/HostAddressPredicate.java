package com.alipay.oceanbase.rule;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: HostAddressPredicate.java, v 0.1 2013-5-24 下午12:54:46 liangjie.li Exp $
 */
public class HostAddressPredicate implements Predicate {

    public static String      localHost    = "";
    public final List<String> hostsPattern = new ArrayList<String>();

    static {
        try {
            localHost = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException ex) {
            // ignore
        }
    }

    public HostAddressPredicate(String host) {
        hostsPattern.addAll(Arrays.asList(host.split(",")));
    }

    @Override
    public boolean needUpdate() {
        return hostsPattern.contains(localHost);
    }

}