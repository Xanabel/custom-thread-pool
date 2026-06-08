package org.example;

public class NamedTask implements Runnable {

    private final String name;
    private final long workTimeMs;

    public NamedTask(String name, long workTimeMs) {
        this.name = name;
        this.workTimeMs = workTimeMs;
    }

    @Override
    public void run() {
        try {
            System.out.println("[Task] " + name + " started.");
            Thread.sleep(workTimeMs);
            System.out.println("[Task] " + name + " finished.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Task] " + name + " interrupted.");
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
