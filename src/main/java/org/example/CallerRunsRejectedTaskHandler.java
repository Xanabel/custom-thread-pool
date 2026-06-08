package org.example;

public class CallerRunsRejectedTaskHandler implements RejectedTaskHandler {

    @Override
    public void reject(Runnable task) {
        System.out.println("[RejectedPolicy] Running task in caller thread: " + task);
        task.run();
    }
}