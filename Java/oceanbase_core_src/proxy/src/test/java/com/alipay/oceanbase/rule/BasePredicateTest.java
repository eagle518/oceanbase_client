package com.alipay.oceanbase.rule;

import java.net.InetAddress;
import java.net.UnknownHostException;
import junit.framework.Assert;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BasePredicateTest {

    @Test
    public void testAndPredicate_true_true() {
        Predicate A = mock(Predicate.class);
        when(A.needUpdate()).thenReturn(Boolean.TRUE);
        Predicate B = mock(Predicate.class);
        when(B.needUpdate()).thenReturn(Boolean.TRUE);
        Assert.assertTrue(new AndPredicate(A, B).needUpdate());
    }

    @Test
    public void testAndPredicate_true_false() {
        Predicate A = mock(Predicate.class);
        when(A.needUpdate()).thenReturn(Boolean.TRUE);
        Predicate B = mock(Predicate.class);
        when(B.needUpdate()).thenReturn(Boolean.FALSE);
        Assert.assertFalse(new AndPredicate(A, B).needUpdate());
        Assert.assertFalse(new AndPredicate(B, A).needUpdate());
    }

    @Test
    public void testAndPredicate_false_false() {
        Predicate A = mock(Predicate.class);
        when(A.needUpdate()).thenReturn(Boolean.FALSE);
        Predicate B = mock(Predicate.class);
        when(B.needUpdate()).thenReturn(Boolean.FALSE);
        Assert.assertFalse(new AndPredicate(A, B).needUpdate());
    }

    @Test
    public void testBooleanPredicate_false() {
        BooleanPredicate booleanPredicate2 = new BooleanPredicate(false);
        Assert.assertFalse(booleanPredicate2.needUpdate());
    }

    @Test
    public void testBooleanPredicate_true() {
        BooleanPredicate booleanPredicate = new BooleanPredicate(true);
        Assert.assertTrue(booleanPredicate.needUpdate());
    }

    @Test
    public void testHostAddressPredicate_host() throws UnknownHostException {
        String localHost = InetAddress.getLocalHost().getHostAddress();
        Predicate hostPredicate = new HostAddressPredicate(localHost);
        Assert.assertTrue(hostPredicate.needUpdate());
    }

    @Test
    public void testHostAddressPredicate_hosts() throws UnknownHostException {
        String localHost = InetAddress.getLocalHost().getHostAddress();
        Predicate hostPredicate = new HostAddressPredicate(localHost + "," + "126.0.0.5");
        Assert.assertTrue(hostPredicate.needUpdate());
    }

    @Test
    public void testPercentPredicate_0() {
        PercentagePredicate percentPredicate2 = new PercentagePredicate(0);
        Assert.assertFalse(percentPredicate2.needUpdate());
    }

    @Test
    public void testPercentPredicate_100() {
        PercentagePredicate percentPredicate = new PercentagePredicate(100);
        Assert.assertTrue(percentPredicate.needUpdate());
    }

    @Test
    public void testOrPredicate_true_true() {
        Predicate A = mock(Predicate.class);
        when(A.needUpdate()).thenReturn(Boolean.TRUE);
        Predicate B = mock(Predicate.class);
        when(B.needUpdate()).thenReturn(Boolean.TRUE);
        Assert.assertTrue(new OrPredicate(A, B).needUpdate());
    }

    @Test
    public void testOrPredicate_true_false() {
        Predicate A = mock(Predicate.class);
        when(A.needUpdate()).thenReturn(Boolean.TRUE);
        Predicate B = mock(Predicate.class);
        when(B.needUpdate()).thenReturn(Boolean.FALSE);
        Assert.assertTrue(new OrPredicate(A, B).needUpdate());
        Assert.assertTrue(new OrPredicate(B, A).needUpdate());
    }

    @Test
    public void testOrPredicate_false_false() {
        Predicate A = mock(Predicate.class);
        when(A.needUpdate()).thenReturn(Boolean.FALSE);
        Predicate B = mock(Predicate.class);
        when(B.needUpdate()).thenReturn(Boolean.FALSE);
        Assert.assertFalse(new OrPredicate(A, B).needUpdate());
    }
}