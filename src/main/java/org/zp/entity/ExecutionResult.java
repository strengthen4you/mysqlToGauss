package org.zp.entity;

/**
 * @Author zp
 * @Date 2024/10/14 14:59
 */

public class ExecutionResult {

    private final boolean success;
    private final String message;
    private final Exception error;

    public ExecutionResult(boolean success, String message, Exception error) {
        this.success = success;
        this.message = message;
        this.error = error;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }

    public Exception getError() {
        return error;
    }
}
