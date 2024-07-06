package io.confluent.csta.timestamp.transforms;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TestDate {
    
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date testDate=sdf.parse("0001-01-01");
        System.out.println(getDaysBeforeEpoch(testDate));

    }

    public static long getDaysBeforeEpoch(Date date) {
        Calendar epochCalendar = Calendar.getInstance();
        epochCalendar.setTimeInMillis(0); 
        Calendar dateCalendar = Calendar.getInstance();
        dateCalendar.setTime(date);
        long differenceInMillis = epochCalendar.getTimeInMillis() - dateCalendar.getTimeInMillis();
        long daysBeforeEpoch = differenceInMillis / (1000 * 60 * 60 * 24);
        return daysBeforeEpoch;
    }
}
