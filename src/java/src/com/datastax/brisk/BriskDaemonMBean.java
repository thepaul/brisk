package com.datastax.brisk;

public interface BriskDaemonMBean {

    /**
     * Return the version of Brisk that is running.
     */
    public String getReleaseVersion();
}
