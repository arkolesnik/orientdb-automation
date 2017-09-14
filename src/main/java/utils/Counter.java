package utils;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Counter {

    private static final int START_VALUE = 0;
    private static final int END_VALUE = 600000000;
    private static final int MIN = 2;
    private static final int MAX = 10;

    private static AtomicInteger integerValue = new AtomicInteger(0);
    private static AtomicInteger id = new AtomicInteger(0);

    private static AtomicInteger instanceCounter = new AtomicInteger(0);
    private static AtomicLong duplicationCounter = new AtomicLong(0);
    private static ConcurrentLinkedQueue<Integer> deletedIds = new ConcurrentLinkedQueue<>();

    public static Integer returnNextInt() {
        return integerValue.incrementAndGet();
    }

    public static Integer returnNextId() {
        return id.incrementAndGet();
    }

    public static Integer generateInt() {
        return ThreadLocalRandom.current().nextInt(START_VALUE, END_VALUE);
    }

    public static int generateSize() {
        return ThreadLocalRandom.current().nextInt(MIN, MAX + 1);
    }

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

    public static ConcurrentLinkedQueue<Integer> getDeletedIds() {
        return deletedIds;
    }
}
