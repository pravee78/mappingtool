
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

public class FeedIntro {
    public static void main(String args[]) throws JSONException{
String jsonstring= "{\"m\":\"2\",\"fs\":\"hour\",\"fts\":\"0\",\"rs\":\"previousmonth\",\"r\":0,\"furl\":\"ddd\",\"fun\":\"admin\",\"fpw\":\"sw33t\",\"ne\":\"sdsd@sss.com\",\"cf\":[\"control\"]}";
JSONObject jsonObj = new JSONObject(jsonstring);

String frequency=(String) jsonObj.get("fs");
String freqtime=(String)jsonObj.get("fts");
//System.out.println(frequency);
//System.out.println(freqtime);
//System.out.println(range);
//System.out.println(days);

Calendar cal = Calendar.getInstance();
Date date = new Date();
SimpleDateFormat simpleDateformat = new SimpleDateFormat("EEEE"); // the day of the week spelled out completely
//System.out.println(simpleDateformat.format(date));
cal.setTime(date);
int hours = cal.get(Calendar.HOUR_OF_DAY);
int minutes = cal.get(Calendar.MINUTE);
int day = cal.get(Calendar.DAY_OF_MONTH);
System.out.println(Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH));
//System.out.println("hours:" +hours);
//System.out.println("minutes:" +minutes);
//System.out.println("days:" +day);
if(frequency.equalsIgnoreCase("hour")){
    if(Integer.parseInt(freqtime)==cal.get(Calendar.MINUTE)){
        //return true;
    System.out.println("Minute success in hour");
    System.out.println(Integer.parseInt(freqtime));
    }
    else{
        //return false;
        System.out.println("Minute fail in hour");
        System.out.println(Integer.parseInt(freqtime));
    }
}
else if (frequency.equalsIgnoreCase("week")){
    if(freqtime.equalsIgnoreCase(simpleDateformat.format(date)) && hours==12){
        System.out.println("Day success in week");
        System.out.println(freqtime);
    }
    else {
        //return false;
        System.out.println("Day failure in week");
        System.out.println(freqtime);
    }
}
else{
if(Integer.parseInt(freqtime)>Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH)){
if(day==Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH) && hours==12){
            System.out.println("Date success in month");
            System.out.println(Integer.parseInt(freqtime));
            //return true;
        }
    }
    else if(Integer.parseInt(freqtime)==day && hours==12){
        System.out.println("Date success in month");
        System.out.println(Integer.parseInt(freqtime));
        //return true;
    }
    else {
        System.out.println("Date failure in month");
        System.out.println(Integer.parseInt(freqtime));
        //return false;
    }

}
    }
}
