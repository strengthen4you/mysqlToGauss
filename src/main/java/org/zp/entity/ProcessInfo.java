package org.zp.entity;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author zp
 * @Date 2024/10/16 11:07
 */

public class ProcessInfo implements Serializable {

    public static ConcurrentHashMap<String, ProcessInfo> taskProgressMap = new ConcurrentHashMap<>();

    public static ConcurrentHashMap<String, ProcessInfo> getTaskProgressMap() {
        return taskProgressMap;
    }

    public enum TaskStatus {
        INIT,
        RUNNING,
        SUCCESS,
        FAIL
    }

    public enum TaskType {
        GENERATE_SQL,
        EXECUTE_SQL,
        TRANSFER_DATA
    }

    private Double process;

    private TaskStatus taskStatus;

    private TaskType taskType;

    public ProcessInfo(Double process, TaskStatus taskStatus, TaskType taskType) {
        this.process = process;
        this.taskStatus = taskStatus;
        this.taskType = taskType;
    }


    public Double getProcess() {
        return process;
    }

    public void setProcess(Double process) {
        this.process = process;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }
}
