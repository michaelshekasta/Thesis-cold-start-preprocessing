package DAL.utils;

import java.io.File;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class EmailUtils
{
    public static void sendEmail(String username, String password, String toEmail, String subject, String filepath, String emailBody)
    {

        Properties props = new Properties();
        props.put("mail.smtp.auth", true);
        props.put("mail.smtp.starttls.enable", true);
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props, new javax.mail.Authenticator()
        {
            protected javax.mail.PasswordAuthentication getPasswordAuthentication()
            {
                return new javax.mail.PasswordAuthentication(username, password);
            }
        });

        try
        {

            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(username));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail));
            message.setSubject(subject);
            message.setText("PFA");

            MimeBodyPart messageBodyPart1 = new MimeBodyPart();
            MimeBodyPart messageBodyPart2 = new MimeBodyPart();

            Multipart multipart = new MimeMultipart();

            messageBodyPart1 = new MimeBodyPart();
            String fileName = filepath.substring(filepath.lastIndexOf("\\") + 1);
            DataSource source = new FileDataSource(filepath);
            messageBodyPart1.setDataHandler(new DataHandler(source));
            messageBodyPart1.setFileName(fileName);
            messageBodyPart2 = new MimeBodyPart();
            messageBodyPart2.setContent(emailBody, "text/html");

            multipart.addBodyPart(messageBodyPart1);
            multipart.addBodyPart(messageBodyPart2);
            message.setContent(multipart);

            System.out.println("Sending");

            Transport.send(message);

            System.out.println("Done");

        } catch (MessagingException e)
        {
            e.printStackTrace();
        }
    }
}

