package utils;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class BasicUtils {

    static final int DAYS_NUMBER = 7;

    public static Date getDateToInterrupt() {
        Date date = new Date();
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(Calendar.DATE, DAYS_NUMBER);
        date.setTime(calendar.getTime().getTime());
        return date;
    }
}
