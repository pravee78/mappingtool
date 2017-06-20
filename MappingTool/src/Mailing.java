import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;

import org.apache.log4j.Logger;

public class Mailing
{
	final Logger log_error = Logger.getLogger("logger_error");
   public boolean SendMail(Stats stat,List<String> toList, String jobname)
   {    
      // Recipient's email ID needs to be mentioned.
     
      // Sender's email ID needs to be mentioned
      String from = "oc-alerts@one-count.com";

      // Assuming you are sending email from localhost
      String host = "mail.gcnpublishing.com";
      
    
      // Get system properties
      Properties properties = System.getProperties();

      // Setup mail server
     // properties.put("mail.smtp.auth", "true");
      //properties.put("mail.smtp.starttls.enable", "true");
      properties.setProperty("mail.smtp.host", host);
    

      // Get the default Session object.
      Session session = Session.getDefaultInstance(properties);

      try{
         // Create a default MimeMessage object.
         MimeMessage message = new MimeMessage(session);

         // Set From: header field of the header.
         message.setFrom(new InternetAddress(from));

         // Set To: header field of the header.
         for(String to:toList)
         message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));

         // Set Subject: header field
         message.setSubject("Job Status for "+jobname);

         // Now set the actual message
         StringBuffer body= new StringBuffer();
         body.append("Job started at "+stat.getStartTime());
         body.append("\nJob completed at "+stat.getEndTime());
         body.append("\nNumber of user records exported: "+stat.getExportedrecords());
         body.append("\nNumber of failed records: "+stat.getFailedrecords());
         message.setText(body.toString());

         // Send message
         Transport.send(message);
         System.out.println("Sent message successfully....");
      }catch (MessagingException mex) {
         mex.printStackTrace();
         log_error.error("Exception while sending mail");
      }
	return false;
   }
}