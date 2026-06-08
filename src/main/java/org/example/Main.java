package org.example;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
                2,
                4,
                5,
                TimeUnit.SECONDS,
                5,
                1,
                "XanaPool",
                new AbortRejectedTaskHandler()
        );

        try {
            Future<String> future = pool.submit(() -> {
                Thread.sleep(1000);
                return "Callable completed successfully";
            });

            System.out.println("[Main] " + future.get());
        } catch (Exception e) {
            System.out.println("[Main] Callable error: " + e.getMessage());
        }

        for (int i = 1; i <= 30; i++) {
            try {
                pool.execute(new NamedTask("Request-" + i, 3000));
            } catch (Exception e) {
                System.out.println("[Main] " + e.getMessage());
            }
        }

        Thread.sleep(15000);

        pool.shutdown();

        System.out.println("[Main] Shutdown requested.");

        Thread.sleep(7000);

        pool.printStatistics();
    }
}