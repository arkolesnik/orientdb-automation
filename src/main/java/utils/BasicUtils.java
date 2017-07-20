package utils;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class BasicUtils {

    private static AtomicLong value = new AtomicLong(0);
    private static int min = 1;
    private static int max = 10;

    public static long returnNextLong() {
        return value.incrementAndGet();
    }

    public static int generateSize() {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

}
