package bdr.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JDBCUtil {

	private static final String url = "jdbc:mysql://192.168.1.100:3306/mx";
	private static final String user = "root";
	private static final String password = "admin";

	static{
		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws SQLException {
		for(int i=0; i<200001; i++){
			String sql = "insert into test1(name) values(?)";
			Connection connection = null;
			try {
				connection = DriverManager.getConnection(url, user, password);
				PreparedStatement ps = connection.prepareStatement(sql);
				ps.setObject(1, "zhangsan");
				ps.addBatch();
				ps.execute();
				
				if(i%10000==0){
					ps.executeUpdate();
					connection.commit();
					ps.clearBatch();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally{
				if(connection!=null){
					try {
						connection.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
	}
	
	public static void update(String sql, Object... params){
		if(params!=null && params.length>0){
			update(sql, Arrays.asList(params));
		}
	}
	/**
	 * 执行insert、update、delete语句
	 * @param sql
	 * @param params
	 */
	public static void update(String sql, List<Object> params){
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, user, password);
			PreparedStatement ps = connection.prepareStatement(sql);
			if(params!=null && params.size()>0){
				for(int i=0; i<params.size(); i++){
					ps.setObject(i+1, params.get(i));
				}
			}
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(connection!=null){
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 查询一行
	 * @param sql
	 * @param params
	 * @return
	 */
	public static List<Object> queryRow(String sql, List<Object> params){
		List<List<Object>> result = queryAll(sql, params);
		return result.get(0);
	}
	/**
	 * 计数
	 * @param sql
	 * @param params
	 * @return
	 */
	public static long count(String sql, List<Object> params){
		List<List<Object>> result = queryAll(sql, params);
		return Long.parseLong(result.get(0).get(0).toString());
	}
	
	/**
	 * 查询
	 * @param sql
	 * @param params
	 * @return
	 */
	public static List<List<Object>> queryAll(String sql, List<Object> params){
		List<List<Object>> result = new ArrayList<List<Object>>();
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, user, password);
			PreparedStatement ps = connection.prepareStatement(sql);
			if(params!=null && params.size()>0){
				for(int i=0; i<params.size(); i++){
					ps.setObject(i+1, params.get(i));
				}
			}
			ResultSet resultset = ps.executeQuery();
			int columnCount = resultset.getMetaData().getColumnCount();
			while(resultset.next()){
				List<Object> line = new ArrayList<Object>();
				for(int i=0; i<columnCount; i++){
					Object value = resultset.getObject(i+1);
					line.add(i, value);
				}
				result.add(line);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(connection!=null){
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
	
	public static List<List<String>> query2(String sql, List<String> params){
		List<List<String>> result = new ArrayList<List<String>>();
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, user, password);
			PreparedStatement ps = connection.prepareStatement(sql);
			if(params!=null && params.size()>0){
				for(int i=0; i<params.size(); i++){
					ps.setObject(i+1, params.get(i));
				}
			}
			ResultSet resultset = ps.executeQuery();
			int columnCount = resultset.getMetaData().getColumnCount();
			while(resultset.next()){
				List<String> line = new ArrayList<String>();
				for(int i=0; i<columnCount; i++){
					Object value = resultset.getObject(i+1);
					if(value!=null){
						//一定要按照位置插入记录。因为有可能前面的列为null
						line.add(i, value.toString());
					}
				}
				result.add(line);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(connection!=null){
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return result;
	}
}
