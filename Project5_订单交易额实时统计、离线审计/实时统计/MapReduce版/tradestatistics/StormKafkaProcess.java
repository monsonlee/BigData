package tsa.tradestatistics;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.mysql.fabric.xmlrpc.base.Value;

import tsa.utils.ENotationUtil;
import tsa.utils.JDBCUtil;
import tsa.utils.TimeUtil;

/**
 * StormKafkaProcess用于处理实时交易统计以及排行榜(Top10)
 *
 * @className StormKafkaProcess
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年11月25日
 */
public class StormKafkaProcess {

	public static void main(String[] args)
			throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

		String topologyName = "TSAS";// 元组名
		// Zookeeper主机地址，会自动选取其中一个
		ZkHosts zkHosts = new ZkHosts("192.168.230.128:2181,192.168.230.129:2181,192.168.230.131:2181");
		String topic = "trademx";
		String zkRoot = "/storm";// storm在Zookeeper上的根路径
		String id = "tsaPro";

		// 创建SpoutConfig对象
		SpoutConfig spontConfig = new SpoutConfig(zkHosts, topic, zkRoot, id);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka", new KafkaSpout(spontConfig), 2);
		builder.setBolt("AccBolt", new AccBolt()).shuffleGrouping("kafka");
		builder.setBolt("ToDbBolt", new ToDbBolt()).shuffleGrouping("AccBolt");

		Config config = new Config();
		config.setDebug(false);

		if (args.length == 0) { // 本地运行，用于测试
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(topologyName, config, builder.createTopology());
			Thread.sleep(1000 * 3600);
			localCluster.killTopology(topologyName);
			localCluster.shutdown();
		} else { // 提交至集群运行
			StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
		}

	}

	/**
	 * AccBolt用于接收Spout发送的消息
	 *
	 * @className AccBolt
	 * @author mxlee
	 * @email imxlee@foxmail.com
	 * @date 2016年11月25日
	 */
	public static class AccBolt extends BaseRichBolt {

		private OutputCollector collector;
		BigDecimal balance = new BigDecimal(0);// 商品总价
		String markTime = null;// 存入mysql的时间

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		public void execute(Tuple input) {
			// 分析信息
			byte[] binary = input.getBinary(0);
			String msg = new String(binary);

			String[] split = msg.split("\t");
			String date = TimeUtil.stamp2Date(split[0]);// 将时间戳1481111901765转为时间2016-12-7
														// 19:58:21
			String subdate = date.substring(0, date.length() - 3);// 获取到分钟2016-12-7
																	// 19:58

			if (markTime != null && !subdate.equals(markTime)) {
				// 将信息发送给下一级bolt，然后由其存入mysql
				Values tuple = new Values(markTime, balance);
				collector.emit(tuple);
				// balance置为0
				balance = new BigDecimal(0);
			}

			markTime = subdate;
			// 单价*数量
			BigDecimal value = new BigDecimal(
					Double.parseDouble(split[3].toString()) * Double.parseDouble(split[4].toString()));
			balance = balance.add(value);// 60s之间的数值之和(第1分钟可能不到60s)
			this.collector.ack(input);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("time", "balance"));
		}

	}

	/**
	 * ToDbBolt用于接收1分钟之内的商品总和，插入mysql
	 *
	 * @className ToDbBolt
	 * @author mxlee
	 * @email imxlee@foxmail.com
	 * @date 2016年12月11日
	 */
	public static class ToDbBolt extends BaseRichBolt {

		private OutputCollector collector;
		BigDecimal balance = new BigDecimal(0);// 商品总价
		String markTime = null;// 存入mysql的时间

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		public void execute(Tuple input) {
			// 分析信息，将时间和总额打入mysql
			markTime = input.getStringByField("time");
			balance = new BigDecimal(input.getStringByField("balance"));
			String sql = "insert into trade(id,time,balance) values(null,?,?)";
			List<Object> params = new ArrayList<Object>();
			params.add(markTime);
			params.add(balance);
			JDBCUtil.update(sql, params);
			this.collector.ack(input);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}

	}

}
