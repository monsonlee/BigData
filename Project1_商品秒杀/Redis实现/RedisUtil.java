package mxlee.ms;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Redis工具类
 * 
 * @author mxlee
 *
 */
public class RedisUtil {
	// 私有化构造方法
	private RedisUtil() {
	}

	private static JedisPool jedisPool = null;

	/**
	 * 获得连接
	 * 
	 * @return
	 */
	public static synchronized JedisPool getJedis() {

		if (jedisPool == null) {
			JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
			// 指定连接池中最大的空闲连接数
			jedisPoolConfig.setMaxIdle(100);
			// 连接池创建的最大连接数
			jedisPoolConfig.setMaxTotal(500);
			// 设置创建连接的超时时间
			jedisPoolConfig.setMaxWaitMillis(1000 * 5);
			// 表示从连接池中获取连接时，先测试连接是否可用
			jedisPoolConfig.setTestOnBorrow(true);
			jedisPool = new JedisPool(jedisPoolConfig, "192.168.1.104", 6379);
		}

		return jedisPool;
	}

	/**
	 * 返回连接
	 * 
	 * @param jedis
	 */
	public static void returnResource(Jedis jedis) {
		jedis.close();
	}

}
