package com.alipay.oceanbase.jdbc.parameter;

/**
 * 
 * 
 * @author liangjie.li
 * @version $Id: ParameterContext.java, v 0.1 Jun 7, 2013 7:20:46 PM liangjie.li Exp $
 */
public class ParameterContext {

    private ParameterMethod parameterMethod = null;
    private Object[]        args            = null;

    public ParameterContext() {
    }

    public ParameterContext(ParameterMethod parameterMethod, Object[] args) {
        this.parameterMethod = parameterMethod;
        this.args = args;
    }

    public ParameterMethod getParameterMethod() {
        return parameterMethod;
    }

    public void setParameterMethod(ParameterMethod parameterMethod) {
        this.parameterMethod = parameterMethod;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(parameterMethod).append("(");
        for (int i = 0; i < args.length; ++i) {
            buffer.append(args[i]);
            if (i != args.length - 1) {
                buffer.append(", ");
            }
        }
        buffer.append(")");

        return buffer.toString();
    }
}
