package mxlee.ms;

import java.net.URL;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Redis秒杀主程序
 * 
 * @author mxlee
 * 
 */
public class RedisMS {

	public static void main(String[] args) {

		JedisPool jedisPool = RedisUtil.getJedis();

		// 预购清单
		String[] arr = { "iphone", "pc", "surface", "mi", "huawei" };

		// Redis数据库赋值
		assignment(arr, 10, jedisPool);

		// 抢购
		panicBuying(arr, 500, jedisPool);

	}

	/**
	 * 为Redis数据库中的商品赋值
	 * 
	 * @param arr
	 *            String 抢购商品数组
	 * @param num
	 *            int 商品库存
	 */
	private static void assignment(String[] arr, int num, JedisPool jedisPool) {

		// 获得连接
		Jedis jedis = jedisPool.getResource();
		boolean flag = false;

		for (int i = 0; i < arr.length; i++) {
			jedis.set(arr[i], num + "");
		}

	}

	/**
	 * 抢购开始
	 * 
	 * @param arr
	 *            String 抢购商品数组
	 * @param threadNum
	 *            int 线程数量
	 */
	private static void panicBuying(String[] arr, int threadNum, JedisPool jedisPool) {
		// 线程池
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(threadNum);

		Random random = new Random();

		for (int i = 0; i < threadNum; i++) {
			// 为线程随机传递需要抢购的商品
			int index = random.nextInt(5);
			RedisThread redisThread = new RedisThread(arr[index], jedisPool);
			fixedThreadPool.submit(redisThread);
		}

		// 关闭线程池
		fixedThreadPool.shutdown();
	}

}
