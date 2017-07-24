package utils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicUtils {

    public static final int startValue = 0;
    public static final int endValue = 600000;
    private static AtomicInteger value = new AtomicInteger(0);
    public static final int min = 2;
    public static final int max = 10;

    public static Integer returnNextInt() {
        return value.incrementAndGet();
    }

    public static Integer generateInt() {
        return ThreadLocalRandom.current().nextInt(startValue, endValue);
    }

    public static int generateSize() {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

}
