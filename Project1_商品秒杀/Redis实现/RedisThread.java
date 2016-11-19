package mxlee.ms;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

public class RedisThread extends Thread {
	private static long start = 0; // 开始抢购时间
	private static long time = 0; // 多长时间抢购一空
	private static int count = 0; // 抢到商品数量
	private JedisPool jedisPool;
	private String pro; // 需要购买的商品

	public RedisThread(String pro, JedisPool jedisPool) {
		this.pro = pro;
		this.jedisPool = jedisPool;
	}

	/**
	 * 打印成功抢购信息
	 * 
	 * @param str
	 */
	private synchronized void print(String str) {
		// 获取程序所在根目录
		Class clazz = RedisMS.class;
		URL url = clazz.getResource("/");
		String path = url.toString();// 结果为file:/D:/Workspaces/javaBasic/nioDemo/target/classes/
		path = path.substring(6);

		// 缓冲写出流
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(path + "/resultRedis.txt", true));
			bw.write(str);
			bw.newLine();
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 线程开始定时器
	 * 
	 * @return
	 */
	private long clock() {

		String clock = "2016-11-18 20:47:00";

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Date date = null;
		try {
			date = simpleDateFormat.parse(clock);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		long time = date.getTime();

		return time;
	}

	@Override
	public void run() {

		// 取当前时间
		long currentTimeMillis = System.currentTimeMillis();

		long millis = clock() - currentTimeMillis;

		if (millis > 0) {
			try {
				Thread.sleep(millis);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		while (true) {

			// 获得连接
			Jedis jedis = jedisPool.getResource();
			try {
				Thread.sleep(100);
			} catch (Exception e) {
			}

			try {
				// 获得此刻商品apple的数量
				int proNum = Integer.parseInt(jedis.get(pro));
				List<Object> result = null;
				// 如果还有库存
				if (proNum > 0) {
					// 监听商品pro
					jedis.watch(pro);
					int proNum1 = Integer.parseInt(jedis.get(pro));

					if (proNum1 < proNum) {
						jedis.unwatch();
					} else {
						// jedis方法开始事务
						Transaction transaction = jedis.multi();

						// 购买商品，然后更改库存
						transaction.set(pro, String.valueOf(proNum - 1));

						// 提交事务
						result = transaction.exec();
					}
					// 监听的商品被别的线程操作，则本线程无法购买商品，需要排队，自己不修改商品的数量
					if (result == null || result.isEmpty()) {
						System.out.println(Thread.currentThread().getName() + "\t正在排队抢购\t" + pro + "...");// 可能是watch-key被外部修改，或者是数据操作被驳回
					} else {
						count++;

						switch (count) {
						case 1:
							start = System.currentTimeMillis();
							break;

						case 50:
							time = System.currentTimeMillis() - start;
							System.out.println("===================" + time);
							break;
						default:
							break;
						}
						String str = Thread.currentThread().getName() + "\t抢购成功，商品名为：\t" + pro + "\t抢购时间："
								+ new Timestamp(new Date().getTime());
						System.out.println(str);
						// 把抢购成功的顾客信息打印出去
						print(str);

					} // end if else

				} else {// 库存为0时
					System.out.println(pro + "已售罄，库存为0");
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
				RedisUtil.returnResource(jedis);
			} finally {
				RedisUtil.returnResource(jedis);
			}

		}
	}
}
