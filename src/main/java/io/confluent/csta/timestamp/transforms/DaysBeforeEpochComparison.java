package io.confluent.csta.timestamp.transforms;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DaysBeforeEpochComparison {

    public static long getDaysBeforeEpochUtil(Calendar dateCalendar) {
        Calendar epochCalendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        epochCalendar.clear();  // Clears all fields
        epochCalendar.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        epochCalendar.set(Calendar.MILLISECOND, 0);  // Ensure no milliseconds

        long differenceInMillis = epochCalendar.getTimeInMillis() - dateCalendar.getTimeInMillis();
        return differenceInMillis / (1000 * 60 * 60 * 24);
    }

    public static void main(String[] args) {
        System.out.println("problematic Date 0001-01-01");
        Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        calendar.clear();  // Clears all fields
        calendar.set(1, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.set(Calendar.ERA, GregorianCalendar.AD); // Ensure the correct era
        calendar.set(Calendar.MILLISECOND, 0); // Ensure no milliseconds

        long daysBeforeEpochUtil = getDaysBeforeEpochUtil(calendar);
        System.out.println("Days before Unix epoch using java.util: " + daysBeforeEpochUtil);
        LocalDate exampleDate = LocalDate.of(1, 1, 1);
        long daysBeforeEpochTime = getDaysBeforeEpoch(exampleDate);
        System.out.println("Days before Unix epoch using java.time: " + daysBeforeEpochTime);

        System.out.println("unproblematic Date 2024-07-07");
        calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        calendar.clear();  // Clears all fields
        calendar.set(2024, Calendar.JULY, 7, 0, 0, 0);
        calendar.set(Calendar.ERA, GregorianCalendar.AD); // Ensure the correct era
        calendar.set(Calendar.MILLISECOND, 0); // Ensure no milliseconds

        daysBeforeEpochUtil = getDaysBeforeEpochUtil(calendar);
        System.out.println("Days before Unix epoch using java.util: " + daysBeforeEpochUtil);
        exampleDate = LocalDate.of(2024, 7, 7);
        daysBeforeEpochTime = getDaysBeforeEpoch(exampleDate);
        System.out.println("Days before Unix epoch using java.time: " + daysBeforeEpochTime);
    }

    public static long getDaysBeforeEpoch(LocalDate date) {
        LocalDate epoch = LocalDate.of(1970, 1, 1);
        return ChronoUnit.DAYS.between(date, epoch);
    }
}
