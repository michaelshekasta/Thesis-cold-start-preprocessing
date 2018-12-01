import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import BL.controllers.CreateDataToExpirement;
import DAL.utils.EmailUtils;

public class Main
{

    public static void main(String[] args) throws Exception
    {
        String message = "Michael you are cool man - finish to make the db";
        LocalDateTime start = LocalDateTime.now();
        try
        {
            CreateDataToExpirement.run();
        } catch (Exception e)
        {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            message = sw.toString(); // stack trace as a string
            System.out.println(message);
        }
        LocalDateTime end = LocalDateTime.now();
        long minutes = ChronoUnit.MINUTES.between(start, end);
        long hours = ChronoUnit.HOURS.between(start, end);
        message = message + "\n StartTime: " + start + "\n End Time: " + end + "\n total time: " + hours + ":" + minutes;
        EmailUtils.sendEmail("shadars003@gmail.com", "123456ABC", "shkasta@post.bgu.ac.il", "finish", "m.txt",
                message);
    }


}
