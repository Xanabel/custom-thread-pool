package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadPool implements CustomExecutor {

    private final int corePoolSize;
    private final int maxPoolSize;
    private final int queueSize;
    private final int minSpareThreads;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;

    private final List<BlockingQueue<Runnable>> queues = new ArrayList<>();
    private final List<Worker> workers = new ArrayList<>();
    private final List<Thread> workerThreads = new ArrayList<>();

    private final CustomThreadFactory threadFactory;
    private final RejectedTaskHandler rejectedTaskHandler;

    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);
    private final AtomicInteger acceptedTasks = new AtomicInteger(0);
    private final AtomicInteger rejectedTasks = new AtomicInteger(0);
    private final AtomicInteger completedTasks = new AtomicInteger(0);

    private volatile boolean shutdown = false;
    private volatile boolean shutdownNow = false;

    public CustomThreadPool(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads,
            String poolName,
            RejectedTaskHandler rejectedTaskHandler
    ) {
        if (corePoolSize <= 0) {
            throw new IllegalArgumentException("corePoolSize must be greater than 0");
        }

        if (maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be greater than or equal to corePoolSize");
        }

        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be greater than 0");
        }

        if (minSpareThreads < 0) {
            throw new IllegalArgumentException("minSpareThreads must not be negative");
        }

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = new CustomThreadFactory(poolName);
        this.rejectedTaskHandler = rejectedTaskHandler;

        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command must not be null");
        }

        if (shutdown) {
            reject(command);
            return;
        }

        ensureSpareThreads();

        BlockingQueue<Runnable> queue = chooseQueue();

        if (queue.offer(command)) {
            acceptedTasks.incrementAndGet();
            int queueId = queues.indexOf(queue);
            System.out.println("[Pool] Task accepted into queue #" + queueId + ": " + command);
            return;
        }

        synchronized (this) {
            if (workers.size() < maxPoolSize) {
                BlockingQueue<Runnable> newQueue = addWorker();

                if (newQueue != null && newQueue.offer(command)) {
                    acceptedTasks.incrementAndGet();
                    int queueId = queues.indexOf(newQueue);
                    System.out.println("[Pool] Task accepted into new queue #" + queueId + ": " + command);
                    return;
                }
            }
        }

        reject(command);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) {
            throw new NullPointerException("callable must not be null");
        }

        FutureTask<T> futureTask = new FutureTask<>(callable);

        Runnable loggingTask = new Runnable() {
            @Override
            public void run() {
                futureTask.run();
            }

            @Override
            public String toString() {
                return "Callable-" + Integer.toHexString(callable.hashCode());
            }
        };

        execute(loggingTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        System.out.println("[Pool] Shutdown started. New tasks will be rejected, queued tasks will finish.");
    }

    @Override
    public void shutdownNow() {
        shutdown = true;
        shutdownNow = true;

        System.out.println("[Pool] ShutdownNow started. Queues will be cleared and workers interrupted.");

        synchronized (this) {
            for (BlockingQueue<Runnable> queue : queues) {
                queue.clear();
            }

            for (Thread thread : workerThreads) {
                thread.interrupt();
            }
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);

        synchronized (this) {
            while (!isTerminated()) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    return isTerminated();
                }
                TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
            }
        }
        return true;
    }

    public boolean isShutdown() {
        return shutdown;
    }

    public boolean isShutdownNow() {
        return shutdownNow;
    }

    public synchronized boolean canStopWorkerOnIdle() {
        return workers.size() > corePoolSize || shutdown;
    }

    public void taskCompleted() {
        completedTasks.incrementAndGet();
    }

    public synchronized void workerTerminated(Worker worker) {
        int index = workers.indexOf(worker);

        if (index >= 0) {
            workers.remove(index);
            queues.remove(index);
            workerThreads.remove(index);
        }

        notifyAll();
    }

    public synchronized boolean isTerminated() {
        return shutdown && workers.isEmpty();
    }

    public void printStatistics() {
        System.out.println();
        System.out.println("========== POOL STATISTICS ==========");
        System.out.println("Accepted tasks: " + acceptedTasks.get());
        System.out.println("Rejected tasks: " + rejectedTasks.get());
        System.out.println("Completed tasks: " + completedTasks.get());
        System.out.println("Active workers: " + workers.size());
        System.out.println("Terminated: " + isTerminated());
        System.out.println("=====================================");
        System.out.println();
    }

    private synchronized BlockingQueue<Runnable> addWorker() {
        if (workers.size() >= maxPoolSize) {
            return null;
        }

        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
        Worker worker = new Worker(queue, keepAliveTime, timeUnit, this);

        Thread thread = threadFactory.newThread(worker);

        workers.add(worker);
        queues.add(queue);
        workerThreads.add(thread);

        thread.start();

        return queue;
    }

    private void ensureSpareThreads() {
        int freeThreads = countFreeThreads();

        synchronized (this) {
            while (freeThreads < minSpareThreads && workers.size() < maxPoolSize) {
                addWorker();
                freeThreads++;
            }
        }
    }

    private synchronized int countFreeThreads() {
        int free = 0;

        for (Worker worker : workers) {
            if (!worker.isBusy()) {
                free++;
            }
        }

        return free;
    }

    private synchronized BlockingQueue<Runnable> chooseQueue() {
        int index = Math.abs(roundRobinIndex.getAndIncrement());
        return queues.get(index % queues.size());
    }

    private void reject(Runnable command) {
        rejectedTasks.incrementAndGet();
        rejectedTaskHandler.reject(command);
    }

    public synchronized Runnable stealTask(Worker thief) {
        for (BlockingQueue<Runnable> otherQueue : queues) {
            if (otherQueue == thief.getQueue()) {
                continue;
            }
            Runnable task = otherQueue.poll();
            if (task != null) {
                return task;
            }
        }
        return null;
    }
}