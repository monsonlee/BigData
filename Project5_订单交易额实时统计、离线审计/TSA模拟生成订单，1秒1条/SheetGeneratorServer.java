package tsa.sheetgenerate;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import redis.clients.jedis.Jedis;
import tsa.utils.JedisUtil;

/**
 * SheetGeneratorServer用于模拟订单服务器，往kafka和日志里打数据
 *
 * @className SheetGeneratorServer
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年12月7日
 */
public class SheetGeneratorServer {
	static final Logger logger = LoggerFactory.getLogger(SheetGeneratorServer.class);
	static ArrayList<String> goodsPriceList = null;// 用于存放商品码与单价信息
	static Random random = new Random();
	static Jedis jedis = null;
	static Producer<String, String> producer = null;
	static KeyedMessage<String, String> message = null;

	public static void main(String[] args) throws IOException {
		readDataFromFile();// 从订单文件，将商品信息读入ArrayList

		// 生成订单
		while (true) {
			gendata();
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// e.printStackTrace();
			}
		}
	}

	/**
	 * 产生数据的格式是：时间戳\t单号\t商品条码\t单价\t数量
	 */
	private static void gendata() {
		// 1.时间戳
		long timestamp = System.currentTimeMillis();

		// 2.获取订单，把单号保存到redis中，使用jedis类操作
		jedis = JedisUtil.getJedis();
		String orderNum = jedis.get("orderNum");
		if (orderNum == null) {
			jedis.set("orderNum", "1");
			orderNum = jedis.get("orderNum");
		} else {
			jedis.incrBy("orderNum", 1);
		}
		String order = StringUtils.leftPad(orderNum, 9, "0");// 假定订单号为9位
		JedisUtil.returnBrokenJedis(jedis);
		jedis = null;

		// 3. 从goodsPriceList中取条码和单价
		int randomIndex = random.nextInt(goodsPriceList.size());
		String code_price = goodsPriceList.get(randomIndex);

		// 4.获得随机数量
		int amount = random.nextInt(100);

		// 5.拼接单据格式 时间戳\t单号\t商品条码\t单价\t数量
		String tradeInfo = timestamp + "\t" + order + "\t" + code_price + "\t" + amount;

		// 6.写入到slf4j和kafka中
		logger.info(tradeInfo);

		try {
			producer = getProducer();
		} catch (IOException e) {
			// e.printStackTrace();
		}
		message = new KeyedMessage<String, String>("trademx", tradeInfo);
		producer.send(message);
		producer.close();
	}

	/**
	 * 从订单文件，将商品信息读入ArrayList
	 * 
	 * @throws IOException
	 */
	private static void readDataFromFile() throws IOException {
		// 读取订单仓库
		InputStream resourceAsStream = SheetGeneratorServer.class.getResourceAsStream("price900");
		List<String> readLines = IOUtils.readLines(resourceAsStream);
		goodsPriceList = new ArrayList<String>();

		for (int i = 1; i < readLines.size(); i++) {
			String[] splited = StringUtils.split(readLines.get(i));
			String code = splited[1];// 条形码
			String priceString = splited[5];// 单价

			// 存入list 条形码 单价
			try {
				double price = Double.parseDouble(priceString);
				goodsPriceList.add(code + "\t" + priceString);
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}
	}

	/**
	 * 获取Kafka生产者
	 * 
	 * @return
	 * @throws IOException 
	 */
	private static Producer<String, String> getProducer() throws IOException {
		Properties originalProps = new Properties();
		originalProps.load(SheetGeneratorServer.class.getResourceAsStream("producer.properties"));
		originalProps.put("serializer.class", "kafka.serializer.StringEncoder");
		Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(originalProps));
		return producer;
	}
}
