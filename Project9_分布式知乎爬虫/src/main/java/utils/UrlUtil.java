package utils;

import redis.clients.jedis.Jedis;

public class UrlUtil {
	public static void juageUrl(String userHref) {
		// 用户url去重
		String md5 = MD5Filter.md5(userHref);
		Jedis jedis = JedisUtil.getJedis();
		if (jedis.get(md5) == null) {
			jedis.append(md5, "md5url");
			jedis.lpush(JedisUtil.urlkey, userHref);
			JedisUtil.returnResource(jedis);
		} else {
			JedisUtil.returnResource(jedis);
		}
	}
}
