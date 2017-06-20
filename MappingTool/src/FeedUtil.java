import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.json.JSONObject;

public class FeedUtil {
	public boolean runcheck(String input) {
		JSONObject jsonObj = new JSONObject(input);
		System.out.println(input);
		String frequency = (String) jsonObj.get("fs");
		String freqtime = (String) jsonObj.get("fts");
			boolean testing = false;
		if (testing)
			return testing;
		Calendar cal = Calendar.getInstance();
		Date date = new Date();
		SimpleDateFormat simpleDateformat = new SimpleDateFormat("EEEE");
		cal.setTime(date);
		int day = cal.get(Calendar.DAY_OF_MONTH);
		System.out.println(Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH));
		if (frequency.equalsIgnoreCase("daily")) {
			return true;
		
		} else if (frequency.equalsIgnoreCase("week")) {
			if (freqtime.equalsIgnoreCase(simpleDateformat.format(date)) ) return true;
			
		} else {
			if (Integer.parseInt(freqtime) > Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH)) {
				if (day == Calendar.getInstance().getActualMaximum(Calendar.DAY_OF_MONTH) ) {
					System.out.println("Date success in month");
					System.out.println(Integer.parseInt(freqtime));
					return true;
				}
			} else if (Integer.parseInt(freqtime) == day ) {
				System.out.println("Date success in month");
				System.out.println(Integer.parseInt(freqtime));
				return true;
			} 

		}
		return false;
	}
}
