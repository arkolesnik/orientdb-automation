package utils;

import java.util.concurrent.ThreadLocalRandom;

public class BasicUtils {

    //TODO: add synchronization
    private static volatile long value = 0;
    private static int min = 1;
    private static int max = 10;

    public static synchronized long returnNextLong() {
        return value++;
    }

    public static int generateSize() {
        return ThreadLocalRandom.current().nextInt(min, max + 1);
    }

}
