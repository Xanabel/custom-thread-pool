package org.example;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class Worker implements Runnable {

    private final BlockingQueue<Runnable> queue;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final CustomThreadPool pool;

    public Worker(
            BlockingQueue<Runnable> queue,
            long keepAliveTime,
            TimeUnit timeUnit,
            CustomThreadPool pool
    ) {
        this.queue = queue;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.pool = pool;
    }

    public BlockingQueue<Runnable> getQueue() {
        return queue;
    }

    @Override
    public void run() {
        String workerName = Thread.currentThread().getName();

        try {
            while (true) {
                if (pool.isShutdownNow()) {
                    break;
                }

                if (pool.isShutdown() && queue.isEmpty()) {
                    break;
                }

                Runnable task = queue.poll(keepAliveTime, timeUnit);

                if (task == null) {
                    if (pool.canStopWorkerOnIdle()) {
                        System.out.println("[Worker] " + workerName + " idle timeout, stopping.");
                        break;
                    }

                    continue;
                }

                if (!pool.isShutdownNow()) {
                    System.out.println("[Worker] " + workerName + " executes " + task);
                    task.run();
                    pool.taskCompleted();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Worker] " + workerName + " interrupted.");
        } finally {
            pool.workerTerminated(this);
            System.out.println("[Worker] " + workerName + " terminated.");
        }
    }
}