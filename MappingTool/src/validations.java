import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import org.apache.http.conn.util.InetAddressUtils;
import org.jsoup.Jsoup;

public class validations {
    public static boolean isOcidhash(String input){
        //Checks whether the value is a valid ocid hash
if((input.length()==64)&&(input.matches("[0-9a-fA-F]+"))){
            return true;
        }
        else
            return false;
    }
    public static boolean isNumeric(String input){
        //Checks whether the value is a valid ocid numeric
        return input.matches("((-|\\+)?[0-9]+(\\.[0-9]+)?)");
    }
    public static String sanitizeOcidhash(String input){
        //Removes invalid characters and return the value
        return input.replaceAll("[^a-zA-Z0-9]","");
    }
    public static String sanitizeOcidnumeric(String input){
        //Removes invalid characters and return the value
        return input.replaceAll("[^0-9.]","");
    }
    public static boolean isInt(String input){
        //Checks whether the value is a valid Int
        return input.matches("\\d+");
    }
    public static String sanitizeInt(String input){
        //Removes invalid characters and return the Int value
        return input.replaceAll("[^0-9]","");
    }
    public static boolean isBinary(String input){
        //Checks whether the value is a valid Binary
        return input.matches("[01]+");
    }
    public static boolean isOctal(String input){
        //Checks whether the value is a valid Octal number
        return input.matches("[0-7]+");
    }
    public static boolean isHex(String input){
        //Checks whether the value is a valid Hex number
        return input.matches("[0-9a-fA-F]+");
    }
    public static boolean isAlphanumeric(String input){
        //Checks whether the value is a valid alphanumeric
        return input.matches("[0-9a-zA-Z]+");
    }
    public static boolean isAlphanumericwspace(String input){
        //Checks whether the value is a valid alphanumeric with space
        return input.matches("[0-9a-zA-Z ]+");
    }
    public static boolean isBoolean(String input){
        //Checks whether the value is a valid Hex number
        return input.matches("[0-1]+");
    }
    public static boolean isLetterswspace(String input){
        //Checks whether the value is a letters only with space
        return input.matches("[a-zA-Z ]+");
    }
    public static boolean lettersonly(String input){
        //Checks whether the value is a letters only
        return input.matches("[a-zA-Z]+");
    }
    public static boolean required(String input){
        boolean check = true;
        //Checks whether the value is null
        if(input.equals(null) || input.equals("")){
            check = false;
        }
        return check;
    }
    public static String sanitizeString(String input){
        //Remove html tags from the string and return the new value
        return Jsoup.parse(input).text();
    }
    public static String rmvequotes(String input){
        //Remove quotes from the string and return the new value
        return input.substring(1, input.length()-1);
    }
    public static boolean isValidemail(String input){
        //Checks whether the value is valid email
        InternetAddress emailAddr;
        boolean check = true;
        try {
            emailAddr = new InternetAddress(input);
            emailAddr.validate();
        } catch (AddressException e) {
            check = false;
        }
        return check;
    }
    public static boolean isValidip(String input){
        //Checks whether the value is valid email
        return InetAddressUtils.isIPv4Address(input) || InetAddressUtils.isIPv6Address(input);
    }
    public static boolean isValidurl(String input){
        //Checks whether the value is valid url
        boolean check = true;
        try {
            URL url = new URL(input);
            URLConnection conn = url.openConnection();
            conn.connect();
        } catch (IOException e) {
            check = false;
        }
        return check;
    }
    public static boolean isValidzip(String input){
        //Checks whether the value is valid US or Canada zip code
        boolean check = false;
        if(input.matches("\\d{5}([ \\-]\\d{4})?") || input.matches("[ABCEGHJKLMNPRSTVXY]\\d[ABCEGHJ-NPRSTV-Z][ ]?\\d[ABCEGHJ-NPRSTV-Z]\\d")){
        check = true;
        }
        return check;
        }
    }
