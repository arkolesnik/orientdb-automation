package utils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicUtils {

    static final int DAYS_NUMBER = 7;
    static final int START_VALUE = 0;
    static final int END_VALUE = 600000;
    private static AtomicInteger value = new AtomicInteger(0);
    static final int MIN = 2;
    static final int MAX = 10;

    public static Date getDateToInterrupt() {
        Date date = new Date();
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, DAYS_NUMBER);
        date.setTime(calendar.getTime().getTime());
        return date;
    }

    public static Integer returnNextInt() {
        return value.incrementAndGet();
    }

    public static Integer generateInt() {
        return ThreadLocalRandom.current().nextInt(START_VALUE, END_VALUE);
    }

    public static int generateSize() {
        return ThreadLocalRandom.current().nextInt(MIN, MAX + 1);
    }

}
