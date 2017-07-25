package utils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class BasicUtils {

    public static final int DAYS_NUMBER = 7;
    public static final int START_VALUE = 0;
    public static final int END_VALUE = 600000;
    private static AtomicInteger value = new AtomicInteger(0);
    public static final int MIN = 2;
    public static final int MAX = 10;

    public static Date getDateToInterrupt() {
        Date date = new Date();
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        //TODO: change minutes to days and use DAYS_NUMBER
        calendar.add(Calendar.MINUTE, 3);
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
