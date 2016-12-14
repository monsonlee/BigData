package tsa.audit;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import clojure.main;
import tsa.utils.HiveUtil;
import tsa.utils.JDBCUtil;

/**
 * TradeAudit用于交易审计
 *
 * @className TradeAudit
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年12月7日
 */
public class TradeAudit {
	
	public static void main(String[] args) throws SQLException {
		audit("2016-12-11 17:17");
	}
	
	/**
	 * 审计mysql中与hive中的交易
	 * 
	 * @param minute
	 *            2016-12-7 19:58
	 * @return
	 * @throws SQLException
	 */
	public static void audit(String minute) throws SQLException {
		// 1.查询MySQL
		String sql = "select balance from trade where time=?";
		List<Object> params = new ArrayList<Object>();
		params.add(minute);
		List<Object> listBalance = JDBCUtil.queryRow(sql, params);
		BigDecimal mysqlBalance = new BigDecimal(listBalance.get(0).toString());
		System.out.println("实时计算:￥" + mysqlBalance);
		// 2.查询Hive的时候，范围是[20161207094900, 20161207095000)
		BigDecimal hiveBalance = HiveUtil.querySale(minute);

		// 3.判断两个结果是否相等
		System.out.println("落地数据:￥" + hiveBalance);
		System.out.println("审计结果是否相等:" + mysqlBalance.equals(hiveBalance));
	}
}
