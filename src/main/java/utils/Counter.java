package utils;

import java.util.concurrent.atomic.AtomicInteger;

public class Counter {

    private static AtomicInteger instanceCounter = new AtomicInteger(0);
    private static AtomicInteger duplicationCounter = new AtomicInteger(0);

    public static void incrementInstance() {
        instanceCounter.incrementAndGet();
    }

    public static void decrementInstance() {
        instanceCounter.decrementAndGet();
    }

    public static int getInstanceNumber() {
        return instanceCounter.get();
    }

    public static void incrementDuplicate() {
        duplicationCounter.incrementAndGet();
    }

    public static int getDuplicationNumber() {
        return duplicationCounter.get();
    }

}
