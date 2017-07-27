package utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Counter {

    private static AtomicInteger instanceCounter = new AtomicInteger(0);
    private static AtomicLong duplicationCounter = new AtomicLong(0);

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

    public static long getDuplicationNumber() {
        return duplicationCounter.get();
    }

}
