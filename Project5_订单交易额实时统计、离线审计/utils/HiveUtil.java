package tsa.utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveUtil {

	static {
		try {
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static BigDecimal querySale(String date) throws SQLException {
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.230.128:10000/tsa", "", "");
		String sql = "select price,amount from tsa.trade where time like ?";
		PreparedStatement preparedStatement = con.prepareStatement(sql);
		preparedStatement.setString(1, '%' + date + '%');
		ResultSet resultSet = preparedStatement.executeQuery();
		BigDecimal balance = new BigDecimal(0);
		while (resultSet.next()) {
			String price = resultSet.getString("price");
			String amount = resultSet.getString("amount");
			// 单价*数量
			BigDecimal value = new BigDecimal(
					Double.parseDouble(price) * Double.parseDouble(amount));
			balance = balance.add(value);// 60s之间的数值之和(第1分钟可能不到60s)
		}
		con.close();
		return balance;
	}

}
