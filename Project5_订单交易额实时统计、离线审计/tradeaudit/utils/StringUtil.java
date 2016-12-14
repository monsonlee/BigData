package tsa.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import redis.clients.jedis.Jedis;

/**
 * StringUtil工具类用于处理String字符串
 *
 * @className StringUtil
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年11月25日
 */
public class StringUtil {

	static String regEx = "[\u4e00-\u9fa5]";
	static Pattern pat = Pattern.compile(regEx);

	/**
	 * 取出单价和商品数量相乘，得到总价返回
	 * 
	 * @param msg
	 * @return
	 */
	public static double multiBalance(String msg) {

		try {
			// 以:切割字符串  	时间戳\t单号\t商品条码\t单价\t数量
			// 1481111901694   000000149       6921168509256   1       85
			String[] split = msg.split("\t");
			// 取出单价和商品数量相乘，得到总价返回
			return Double.parseDouble(split[3].toString()) * Double.parseDouble(split[4].toString());
		} catch (Exception e) {
			return 0;
		}

	}

	/**
	 * 判断是否包含汉字
	 * 
	 * @param msg
	 * @return
	 */
	public static boolean hasChinese(String msg) {
		Matcher matcher = pat.matcher(msg);
		boolean flg = false;
		if (matcher.find()) {
			flg = true;
		}
		return flg;
	}

	/**
	 * 截取字符串，判断是要查几分钟的
	 * 
	 * @param str
	 * @return
	 */
	public static String isMinite(String str) {
		int indexOf = str.indexOf("=");
		if (indexOf == -1) {
			return null;
		}
		return str.substring(indexOf + 1);
	}

	/**
	 * 将信息中包含的商品码，总金额添加进map
	 * 
	 * @param hashMap
	 * @param msg
	 * @return
	 */
	public static Map<String, String> addMap(Map<String, String> hashMap, String msg) {

		String[] split = msg.split(":");

		if (split == null) {
			return hashMap;
		}

		double balance = Double.parseDouble(split[3].toString()) * Double.parseDouble(split[4].toString());// 总金额

		// 判断Map中是否已经包含该商品码
		if (hashMap.containsKey(split[2])) {
			String value = hashMap.get(split[2]);
			balance += Double.parseDouble(value);
			hashMap.put(split[2], balance + "");
		} else {
			hashMap.put(split[2], balance + "");
		}

		return hashMap;
	}

	/**
	 * 遍历Map，将Map中的商品依次推进Redis sorted set
	 * 
	 * @param jedis
	 * @param currentTime
	 * @param hashMap
	 */
	public static void map2Redis(long currentTime, Map<String, String> hashMap) {
		Jedis jedis = new Jedis("192.168.1.104", 6379);
		// 遍历Map
		Set<String> keySet = hashMap.keySet();
		Iterator<String> iterator = keySet.iterator();

		while (iterator.hasNext()) {
			String key = iterator.next();// 获得商品码
			String value = hashMap.get(key);// 获得字符串类型的总金额
			jedis.zadd("mx@" + currentTime, Double.parseDouble(value), key);
		}

		jedis.close();
	}

}
