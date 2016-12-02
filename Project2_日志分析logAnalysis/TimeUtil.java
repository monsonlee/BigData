package mx1202.wla1;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {

	/**
	 * 根据给定字符串，转成时间戳，格式yyyy-MM-dd HH:mm:ss
	 * 
	 * @param str
	 * @return
	 * @throws ParseException
	 */
	public static long transDate(String str) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date parse = null;
		try {
			parse = simpleDateFormat.parse(str);
		} catch (ParseException e) {
			return -1;
		}
		long time = parse.getTime();
		return time;
	}

}
