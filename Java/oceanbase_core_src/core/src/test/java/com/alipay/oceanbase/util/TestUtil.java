package com.alipay.oceanbase.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: TestUtil.java, v 0.1 Jun 21, 2013 12:31:36 PM liangjie.li Exp $
 */
public class TestUtil {
    public static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, newValue);
    }
}
