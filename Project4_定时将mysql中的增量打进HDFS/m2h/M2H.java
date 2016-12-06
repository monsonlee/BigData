package mx1130.m2h;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import redis.clients.jedis.Jedis;

/**
 * M2H用于定时将新插入mysql的数据，实时更新到hdfs中，mysql中的数据3分钟更新一次
 *
 * @className M2H
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年11月30日
 */
public class M2H {

	private static final String url = "jdbc:mysql://192.168.1.100:3306/test?useUnicode=true&characterEncoding=utf-8";
	private static final String user = "root";
	private static final String password = "admin";
	static FileSystem fileSystem = null;
	static int hehe = 0;
	static {
		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * HDFS的访问地址，是使用HDFS协议的地址
	 */
	private static final String URIPATH = "hdfs://192.168.1.107:9000";

	public static void main(String[] args) throws SQLException, URISyntaxException, IOException {
		fileSystem = getInstance();
		// 从mysql中读取更新的数据，返回ResultSet
		Jedis jedis = new Jedis("192.168.1.107", 6379);
		String num = jedis.get("mark");
		jedis.close();
		int count = 0;
		if (num != null) {
			count = Integer.parseInt(num);
		}
		String sql = "select * from test1 limit ?,99999999";

		// 处理mysql to hdfs
		queryRS(sql, count);
	}

	/**
	 * 相当于-put命令，往HDFS拷贝文件，上传
	 * 
	 * @param config
	 * @param fileSystem
	 * @param inputStream
	 * @param bytes
	 * @param path
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws URISyntaxException
	 */
	private static void put(FileSystem fileSystem, ByteArrayInputStream inputStream, byte[] bytes)
			throws IOException, FileNotFoundException, URISyntaxException {
		Path path = new Path("/mxlee/m2h.txt");
		FSDataOutputStream outputStream = null;
		if (fileSystem.exists(path)) {
			outputStream = fileSystem.append(path);
			outputStream.write(bytes);

		} else {
			outputStream = fileSystem.create(path);
			outputStream.write(bytes);
		}

		outputStream.close();
		// IOUtils.copyBytes(inputStream, outputStream, config, true);
		System.out.println("上传成功" + hehe++);
	}

	/**
	 * 获得FileSystem实例
	 * 
	 * @return
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	private static FileSystem getInstance() throws URISyntaxException, IOException {
		Configuration config = new Configuration();
		config.set("dfs.support.append", "true");
		config.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		config.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		URI uri = new URI(URIPATH);
		return FileSystem.get(uri, config);
	}

	/**
	 * 从mysql中读取更新的数据，返回ResultSet
	 * 
	 * @param sql
	 * @param count
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws URISyntaxException
	 */
	public static void queryRS(String sql, int count) throws FileNotFoundException, IOException, URISyntaxException {
		Connection connection = null;
		ResultSet resultset = null;
		StringBuilder sb = new StringBuilder();

		try {
			connection = DriverManager.getConnection(url, user, password);
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setInt(1, count);
			resultset = ps.executeQuery();
			// 2、解析rs
			int columnNum = resultset.getMetaData().getColumnCount();
			sb.delete(0, sb.length());
			int id;
			String name;
			String line;
			int num = 0;
			while (resultset.next()) {
				id = resultset.getInt(1);
				name = resultset.getString(2);
				line = id + "\t" + name;
				sb.append(line).append("\r\n");
				num++;
				if (num == 1024 * 10) {
					// 写入hdfs
					byte[] bytes = sb.toString().getBytes();
					ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
					put(fileSystem, inputStream, bytes);
					// StringBuilder清空
					sb.delete(0, sb.length());
				}
			} // end while
			byte[] bytes = sb.toString().getBytes();
			ByteArrayInputStream is = new ByteArrayInputStream(bytes);
			put(fileSystem, is, bytes);
			mark2Redis(count + num);
			num = 0;

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} // end if
		} // end finally

	}// end queryRS

	/**
	 * 往Redis中打印读取标记
	 * 
	 * @param num
	 */
	private static void mark2Redis(int num) {
		Jedis jedis = new Jedis("192.168.1.107", 6379);
		jedis.set("mark", num + "");
		jedis.close();
	}

}
