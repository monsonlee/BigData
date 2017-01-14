package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import domain.User;

public class JDBCUtil {

	private static final String url = "jdbc:mysql://127.0.0.1:3306/zhihu?useUnicode=true&characterEncoding=utf-8";
	private static final String username = "root";
	private static final String password = "123";

	static {
		try {
			DriverManager.registerDriver(new com.mysql.jdbc.Driver());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 执行insert、update、delete语句
	 * 
	 * @param sql
	 * @param params
	 */
	public static void update(String sql, User user) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, username, password);
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, user.getUserID());
			ps.setString(2, user.getName());
			ps.setString(3, user.getGender());
			ps.setString(4, user.getLocation());
			ps.setString(5, user.getBusiness());
			ps.setString(6, user.getEmployment());
			ps.setString(7, user.getPosition());
			ps.setString(8, user.getSchool());
			ps.setString(9, user.getMajor());
			ps.setInt(10, user.getAnswersNum());
			ps.setInt(11, user.getStarsNum());
			ps.setInt(12, user.getThxNum());
			ps.setInt(13, user.getSaveNum());
			ps.setInt(14, user.getFollow());
			ps.setInt(15, user.getFollower());
			ps.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}// end update
}
