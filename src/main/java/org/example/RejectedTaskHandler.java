package org.example;

public interface RejectedTaskHandler {

    void reject(Runnable task);
}