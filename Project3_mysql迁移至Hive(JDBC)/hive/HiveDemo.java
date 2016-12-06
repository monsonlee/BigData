package bdr1205.hive;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import bdr.utils.JDBCUtil;

/**
 * HiveDemo用于把MySQL中大量的表和数据迁移到Hive中
 *
 * @className HiveDemo
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年12月5日
 */
public class HiveDemo {

	static String url = "jdbc:mysql://192.168.1.100:3306/mx";
	static String user = "root";
	static String password = "admin";

	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {

		// 3.1、加载驱动
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		// 3.2、获取连接
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.1.101:10000/mxlee", "", "");
		Statement stmt = con.createStatement();
		// 1、找出数据库中的表
		ArrayList<String> list = queryDBTable();
		// 2、查看数据库表的结构，返回建表语句
		for (String table : list) {
			String sql = generateCreateTable(table);
			// 3、在hive中创建对应的表
			stmt.execute(sql);
		}

		// 4.从MySQL查出数据写入到Hive中
		for (String tableName : list) {
			List<List<Object>> queryAll = queryData(tableName);
			// 调用jdbc写入到Hive中
			// 4.1、将数据写到本地，4.1也可省略，直接用4.2打到HDFS中
			String path = "D:/1.txt";
			FileWriter fileWriter = new FileWriter(new File(path), true);
			URI uri = new URI("hdfs://192.168.1.101:9000");
			FileSystem fileSystem = FileSystem.get(uri, new Configuration());
			// 4.2 、 将本地文件传至HDFS中
			FSDataOutputStream outputStream = fileSystem.create(new Path("/home/group4/mxlee/1205hive/logs/1.txt"));

			for (List<Object> list2 : queryAll) {
				String[] split = list2.toString().split(",");
				String id = split[0].substring(1);
				String province = split[1].substring(0, split[1].length() - 1);
				fileWriter.write(id + "\t" + province + "\r\n");
			}

			fileWriter.close();
			FileInputStream in = new FileInputStream(new File(path));
			IOUtils.copyBytes(in, outputStream, new Configuration(), true);
			System.out.println("上传成功");
		}
		// 4.3、将 HDFS中的文件导入HIVE中、
		String sql = "LOAD DATA INPATH '/home/group4/mxlee/1205hive/logs/1.txt' OVERWRITE INTO TABLE mxlee.m2h";
		stmt.executeQuery(sql);
		con.close();
	}

	/**
	 * 根据表名查找信息
	 * 
	 * @param tableName
	 * @return
	 */
	private static List<List<Object>> queryData(String tableName) {
		String sql = "select * from " + tableName;
		return JDBCUtil.queryAll(sql, null);
	}

	/**
	 * 生成创建表的sql语句
	 * 
	 * @param table
	 * @return
	 * @throws SQLException
	 */
	private static String generateCreateTable(String table) throws SQLException {
		ArrayList<String> arrayList = queryTableInfo(table);
		StringBuilder stringBuilder = new StringBuilder("create table mxlee." + table + "(");
		for (String tableName : arrayList) {
			stringBuilder.append(StringUtils.replaceChars(tableName, "\t", " ")).append(",");
		}
		String substring = stringBuilder.substring(0, stringBuilder.length() - 1);
		substring += ") row format delimited fields terminated by '\\t'";
		return substring;
	}

	/**
	 * 查看数据库表的结构
	 * 
	 * @param table
	 * @return
	 * @throws SQLException
	 */
	private static ArrayList<String> queryTableInfo(String table) throws SQLException {
		Connection connection = DriverManager.getConnection(url, user, password);
		String sql = "select * from " + table;// 根据传进来的表名查询

		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery(sql);

		ResultSetMetaData metaData = resultSet.getMetaData();// 获得查询结果的元数据(此处也指表的元数据)
		int columnCount = metaData.getColumnCount();// 表中的列数

		ArrayList<String> arrayList = new ArrayList<String>();// 存储列名
		for (int i = 1; i <= columnCount; i++) {
			// 类型
			String typeName = "String";
			// 列名
			String name = metaData.getColumnName(i);
			arrayList.add(name + " " + typeName);
		}
		// 关闭连接
		connection.close();

		return arrayList;

	}

	/**
	 * 遍历数据库，获得数据库中的所有表名
	 * 
	 * @return ArrayList
	 * @throws SQLException
	 */
	private static ArrayList<String> queryDBTable() throws SQLException {
		Connection connection = DriverManager.getConnection(url, user, password);
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet tables = metaData.getTables(null, null, null, new String[] { "TABLE" });
		int columnCount = tables.getMetaData().getColumnCount();
		ArrayList<String> arrayList = new ArrayList<String>();
		while (tables.next()) {
			arrayList.add(tables.getObject(3).toString());
		}
		connection.close();
		return arrayList;
	}

}
