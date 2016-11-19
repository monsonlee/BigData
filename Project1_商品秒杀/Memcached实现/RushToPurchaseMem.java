package mxlee.ms;

import java.io.IOException;
import java.net.InetSocketAddress;

import net.spy.memcached.MemcachedClient;

/**
 * 模拟商品抢购，使用memcached缓存，同时开启多个线程访问memcached服务器
 * 
 * @classnaName RushToPurchaseMem.java
 * @author mxlee
 * @date 2016年11月16日
 */
public class RushToPurchaseMem {

	public static void main(String[] args) {
		addProductions();// 加入商品
		System.out.println("开始抢购，时间点：" + System.currentTimeMillis());

		for (int i = 0; i < 500; i++) {// 同时开启多个线程访问memcached服务器 new
			new Thread(new BuyProductionMem()).start();
		}

	}

	// -----此方法向memcached中加入商品数据
	public static void addProductions() {
		String MEMCACHED_SERVER_IP = "192.168.1.104";// 服务器端ip
		int MEMCACEHD_SERVER_PORT = 11211; // 服务器端端口
		MemcachedClient memcachedClient = null;
		try {
			memcachedClient = new MemcachedClient(new InetSocketAddress(MEMCACHED_SERVER_IP, MEMCACEHD_SERVER_PORT));
		} catch (IOException e) {
			System.out.println("链接服务器失败");
			e.printStackTrace();
		}
		// 存入数据
		memcachedClient.set("prod1", 30, 10);// 60表示缓存时间为60秒，60秒后自动销毁此条key-value
		memcachedClient.set("prod2", 30, 10);
		memcachedClient.set("prod3", 30, 10);
		memcachedClient.set("prod4", 30, 10);
		memcachedClient.set("prod5", 30, 10);
		System.out.println(memcachedClient.get("prod1"));
		System.out.println(memcachedClient.get("prod2"));
		System.out.println(memcachedClient.get("prod3"));
		System.out.println(memcachedClient.get("prod4"));
		System.out.println(memcachedClient.get("prod5"));
		memcachedClient.shutdown();
	}
}
