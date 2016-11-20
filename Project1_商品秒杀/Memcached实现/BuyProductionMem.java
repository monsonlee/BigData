package mxlee.ms;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Random;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;

/**
 * 模拟抢购任务，每个任务都会连接服务器，修改商品的值，这里使用了CAS协议，保证了多线程下的数据原子性
 * 
 * @classnaName BuyProductionMem.java
 * @author mxlee
 * @date 2016年11月16日
 */
public class BuyProductionMem implements Runnable {
	private String MEMCACHED_SERVER_IP = "192.168.1.104";// 服务器端ip
	private int MEMCACEHD_SERVER_PORT = 11211; // 服务器端端口
	public static long start = 0;
	public static long times = 0;
	public static int count = 0;

	@Override
	public void run() {
		MemcachedClient memcachedClient = null;
		try { // 新建一个memcached客户端
			memcachedClient = new MemcachedClient(new InetSocketAddress(MEMCACHED_SERVER_IP, MEMCACEHD_SERVER_PORT));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Random randomInt = new Random();
		int nextInt = randomInt.nextInt(5) + 1;
		String productionName = "prod" + nextInt;// 随机得到一个商品名（key）

		CASValue<Object> casValue = memcachedClient.gets(productionName);// 得到key对应的CASValue
		long cas = casValue.getCas();// 得到商品对应的数量的版本号
		Integer value = (Integer) casValue.getValue();// 得到商品对应的数量的值

		if (value > 0) {
			// 通过cas修改value，如果版本号没变则返回OK修改成功，如果版本号变了则返回其他值
			CASResponse response = memcachedClient.cas(productionName, cas, value - 1);
			if (response.toString().equals("OK")) {
				System.out.println(
						Thread.currentThread().getName() + "成功抢到一个商品：" + productionName + "\t剩余：" + (value - 1));
				count++;
				switch (count) {
				case 1:
					start = System.currentTimeMillis();
					break;
				case 50:
					times = System.currentTimeMillis() - start;
					System.out.println("============================" + times);
					break;
				default:
					break;
				}
				print(Thread.currentThread().getName() + "成功抢到一个商品：" + productionName + "\t剩余：" + (value - 1));
			} else {
				System.out.println(Thread.currentThread().getName() + "手速慢了，没抢到");
			}
			System.out.println("时间点" + System.currentTimeMillis());
		} else {
			System.out.println("商品" + productionName + "，已经被抢光了");
		}
		memcachedClient.shutdown();
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
			bw = new BufferedWriter(new FileWriter(path + "/resultMem.txt", true));
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

}
