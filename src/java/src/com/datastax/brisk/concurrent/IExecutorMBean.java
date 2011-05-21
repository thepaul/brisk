package com.datastax.brisk.concurrent;

public interface IExecutorMBean
{
    /**
     * Get the current number of running tasks
     */
    public int getActiveCount();

    /**
     * Get the number of completed tasks
     */
    public long getCompletedTasks();

    /**
     * Get the number of tasks waiting to be executed
     */
    public long getPendingTasks();

}
