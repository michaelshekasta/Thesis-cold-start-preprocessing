package DAL.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

public class DateUtils
{
    public static final DateTimeFormatter patternDateTime = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    //                                                                                   '2002-10-20 12:00:00'
    public static final DateTimeFormatter patternDateTimeDB = DateTimeFormatter.ofPattern("yyy-MM-dd HH:mm:ss", Locale.ENGLISH);

    public static final DateTimeFormatter patternDate = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ENGLISH);

    public static LocalDateTime getDateTimeFromString(String str)
    {
        return LocalDateTime.parse(str, patternDateTime);
    }

    public static LocalDate getDateFromString(String str)
    {
        return LocalDate.parse(str);
    }

    public static String getDateToDB(String timestamp)
    {
        return getDateToDB(timestamp,"");
    }
    public static String getDateToDB(String timestamp, String pattern)
    {
        LocalDateTime dateTime = null;
        if(pattern.length() > 0)
        {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
            dateTime = LocalDateTime.parse(timestamp, formatter);
        }
        else
        {
            dateTime = LocalDateTime.parse(timestamp);

        }
        return dateTime.format(patternDateTimeDB);
    }

    public static String getDateToDBFromISO(String timestamp)
    {
        LocalDateTime dateTime = LocalDateTime.parse(timestamp, BASIC_ISO_DATE);
        return dateTime.format(patternDateTime);
    }

    public static String getDateToDB(LocalDateTime ldt)
    {
        return ldt.format(patternDateTimeDB);
    }

}
