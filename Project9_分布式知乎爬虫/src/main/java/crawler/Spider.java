package crawler;

import java.net.InetAddress;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import domain.Page;
import domain.User;
import download.DownLoad;
import download.DownLoadImpl;
import process.Process;
import process.ProcessImpl;
import redis.clients.jedis.Jedis;
import store.Store;
import store.StoreImpl;
import utils.JedisUtil;
import utils.ThreadUtil;
import utils.UserUtil;

public class Spider {
	static Logger logger = LoggerFactory.getLogger(Spider.class.getSimpleName());
	// 创建一个初始化线程数量为5的线程池
	private static ExecutorService threadPool = Executors.newFixedThreadPool(5);
	private DownLoad downLoadIn;
	private Process processIn;
	private Store storeIn;

	public void setStoreIn(Store storeIn) {
		this.storeIn = storeIn;
	}

	public void setDownLoadInter(DownLoad downLoadIn) {
		this.downLoadIn = downLoadIn;
	}

	public void setProcessInter(Process processIn) {
		this.processIn = processIn;
	}

	public static void main(String[] args) {
		start();
	}

	/*public Spider() {
		// 重试机制：1000：表示重试的间隔，3表示是重试的次数
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		// 指定zk的链接地址
		String zookeeperConnectionString = "192.168.230.128:2181,192.168.230.129:2181,192.168.230.131:2181";
		int sessionTimeoutMs = 5000;// 链接失效时间，默认是40s，注意：这个值只能是4s--40s之间的值
		int connectionTimeoutMs = 3000;// 链接超时时间
		// 获取zk链接
		CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeperConnectionString, sessionTimeoutMs,
				connectionTimeoutMs, retryPolicy);
		// 开启链接
		client.start();
		try {
			// 获取本机ip信息
			InetAddress localHost = InetAddress.getLocalHost();
			String ip = localHost.getHostAddress();
			client.create()// 创建节点
					.creatingParentsIfNeeded()// 如果需要，则创建父节点
					.withMode(CreateMode.EPHEMERAL)// 指定节点类型
					.withACL(Ids.OPEN_ACL_UNSAFE)// 指定节点的权限
					.forPath("/spider/" + ip);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}*/

	/**
	 * 启动爬虫
	 */
	public static void start() {
		final Spider spider = new Spider();
		spider.setDownLoadInter(new DownLoadImpl());
		spider.setProcessInter(new ProcessImpl());
		spider.setStoreIn(new StoreImpl());
		System.out.println("请输入一个要爬取的知乎话题:");
		// 获取话题
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(System.in);
		String topic = scanner.nextLine();// 话题
		String url = "https://www.zhihu.com/search?type=topic&q=" + topic;
		logger.info("爬虫开始运行...");
		// 下载话题精华问题页
		final Page page = spider.download(url);
		// 解析话题精华问题页
		spider.process(page);

		while (true) {
			// 读取Redis中的url
			Jedis jedis = JedisUtil.getJedis();
			final String userUrl = jedis.rpop(JedisUtil.urlkey);
			JedisUtil.returnResource(jedis);
			if (userUrl != null) {
				threadPool.execute(new Runnable() {
					public void run() {
						if (userUrl.endsWith("following") || userUrl.endsWith("follower")) {
							UserUtil.processFollow(userUrl);
						} else {
							User user = UserUtil.processUser(userUrl);
							if (user != null) {
								spider.store(user);// 存储
							}else{
								logger.info("很奇怪，user为null");
							}
						}
					}// end run
				});

			} else {
				logger.info("没有url了，休息一会...");
				ThreadUtil.sleep(5);
			} // end if else

		} // end while

	}

	/**
	 * 下载
	 * 
	 * @param url
	 * @return
	 */
	public Page download(String url) {
		return downLoadIn.download(url);
	}

	/**
	 * 解析爬取的原始内容
	 * 
	 * @param page
	 * @param user
	 */
	public void process(Page page) {
		processIn.process(page);
	}

	/**
	 * 保存解析的用户信息
	 * 
	 * @param user
	 */
	public void store(User user) {
		storeIn.store(user);
	}
}
