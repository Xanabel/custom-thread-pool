package org.example;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========== ТЕСТ 1: Abort Policy (исключение при отказе) ==========");
        testWithPolicy(new AbortRejectedTaskHandler(), true);

        System.out.println("\n\n========== ТЕСТ 2: CallerRuns Policy (выполнение в потоке отправителя) ==========");
        testWithPolicy(new CallerRunsRejectedTaskHandler(), false);

        System.out.println("\n\n========== ТЕСТ 3: Работа с Callable ==========");
        testCallable();
    }

    private static void testWithPolicy(RejectedTaskHandler policy, boolean expectExceptions) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2,
                4,
                5, TimeUnit.SECONDS,
                3,
                1,
                "XanaPool",
                policy
        );

        int rejectedCount = 0;
        int acceptedCount = 0;

        for (int i = 1; i <= 20; i++) {
            try {
                pool.execute(new NamedTask("Request-" + i, 2000));
                acceptedCount++;
            } catch (RejectedExecutionException e) {
                rejectedCount++;
                System.out.println("[Main] Caught rejection for Request-" + i);
            }
        }

        System.out.println("[Main] Accepted: " + acceptedCount + ", Rejected: " + rejectedCount);

        Thread.sleep(10000);

        pool.shutdown();
        System.out.println("[Main] Shutdown requested.");

        Thread.sleep(8000);

        pool.printStatistics();

        System.out.println("[Main] Pool is terminated: " + pool.isTerminated());
    }

    private static void testCallable() {
        CustomThreadPool pool = new CustomThreadPool(
                2, 4, 5, TimeUnit.SECONDS, 5, 1,
                "CallablePool",
                new AbortRejectedTaskHandler()
        );

        try {
            Future<String> future1 = pool.submit(() -> {
                Thread.sleep(1000);
                return "Результат задачи 1";
            });

            Future<Integer> future2 = pool.submit(() -> {
                Thread.sleep(500);
                return 42;
            });

            System.out.println("[Main] Callable results: " + future1.get() + ", " + future2.get());
        } catch (Exception e) {
            System.out.println("[Main] Callable error: " + e.getMessage());
        } finally {
            pool.shutdown();
        }

        System.out.println("[Main] Callable test finished.");
    }
}