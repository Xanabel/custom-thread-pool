package org.example;

import java.util.concurrent.RejectedExecutionException;

public class AbortRejectedTaskHandler implements RejectedTaskHandler {

    @Override
    public void reject(Runnable task) {
        System.out.println("[Rejected] Task " + task + " was rejected due to overload!");
        throw new RejectedExecutionException("Task rejected: " + task);
    }
}