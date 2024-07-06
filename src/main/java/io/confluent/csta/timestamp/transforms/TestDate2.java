package io.confluent.csta.timestamp.transforms;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class TestDate2 {

    public static long getDaysBeforeEpoch(LocalDate date) {
        LocalDate epoch = LocalDate.ofEpochDay(0);
        long daysBeforeEpoch = ChronoUnit.DAYS.between(date, epoch);
        return daysBeforeEpoch;
    }

    public static void main(String[] args) {
        LocalDate exampleDate = LocalDate.of(1, 1, 1);
        long daysBeforeEpoch = getDaysBeforeEpoch(exampleDate);
        System.out.println(daysBeforeEpoch);
    }
}