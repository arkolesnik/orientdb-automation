package utils;

import java.util.concurrent.atomic.AtomicInteger;

public class Counter {

    private static AtomicInteger c = new AtomicInteger(0);

    public static void increment() {
        c.incrementAndGet();
    }

    public static void decrement() {
        c.decrementAndGet();
    }

    public static int value() {
        return c.get();
    }

}
