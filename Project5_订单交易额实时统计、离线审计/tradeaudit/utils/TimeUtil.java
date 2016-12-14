package tsa.utils;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {

	/**
	 * 获取当前时间的时间戳
	 * 
	 * @return
	 */
	public static long currentTime() {
		String str = new Date().toLocaleString();
		long transDate = transDate(str);
		return transDate;
	}

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
			// System.out.println("对不起，未查到相关结果，请检查输入是否有错!");
			return -1;
		}
		long time = parse.getTime();
		return time;
	}

	/**
	 * 将时间戳转为时间
	 * 
	 * @param time
	 * @return
	 */
	public static String stamp2Date(String time) {
		Timestamp timestamp = new Timestamp(Long.parseLong(time));
		return timestamp.toLocaleString();
	}

}
