package tsa.utils;

import java.math.BigDecimal;
import java.text.DecimalFormat;

/**
 * ENotationUtil用于科学计数法的处理
 *
 * @className ENotationUtil
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年11月26日
 */
public class ENotationUtil {

	private ENotationUtil() {
	}

	/**
	 * 进行加法运算
	 * 
	 * @param d1
	 *            double
	 * @param d2
	 *            double
	 * @return double 两者之和
	 */
	public static double add(double d1, double d2) {
		BigDecimal b1 = new BigDecimal(d1);
		BigDecimal b2 = new BigDecimal(d2);
		return b1.add(b2).doubleValue();
	}
	
	/**
	 * 进行加法运算
	 */
	public static BigDecimal addBigDec(BigDecimal b1, BigDecimal b2) {
		return b1.add(b2);
	}

	/**
	 * 进行减法运算
	 * 
	 * @param d1
	 *            double
	 * @param d2
	 *            double
	 * @return double 两者之差
	 */
	public static double sub(double d1, double d2) {
		BigDecimal b1 = new BigDecimal(d1);
		BigDecimal b2 = new BigDecimal(d2);
		return b1.subtract(b2).doubleValue();
	}

	/**
	 * 进行乘法运算
	 * 
	 * @param d1
	 *            double
	 * @param d2
	 *            double
	 * @return double 两者之积
	 */
	public static double mul(double d1, double d2) {
		BigDecimal b1 = new BigDecimal(d1);
		BigDecimal b2 = new BigDecimal(d2);
		return b1.multiply(b2).doubleValue();
	}

	/**
	 * 进行除法运算
	 * 
	 * @param d1
	 *            double
	 * @param d2
	 *            double
	 * @return double
	 */
	public static double div(double d1, double d2, int len) {
		BigDecimal b1 = new BigDecimal(d1);
		BigDecimal b2 = new BigDecimal(d2);
		return b1.divide(b2, len, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	/**
	 * 进行除法运算
	 * 
	 * @param d1
	 *            double
	 * @param d2
	 *            double
	 * @return double
	 */
	public static double transf(double d1, int len) {
		BigDecimal b1 = new BigDecimal(d1);
		BigDecimal b2 = new BigDecimal("100000000");
		return b1.divide(b2, len, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	/**
	 * 进行四舍五入操作
	 * 
	 * @param d1
	 *            double
	 * @param d2
	 *            double
	 * @return double
	 */
	public static double round(double d, int len) {
		BigDecimal b1 = new BigDecimal(d);
		BigDecimal b2 = new BigDecimal(1);
		// 任何一个数字除以1都是原数字，ROUND_HALF_UP是BigDecimal的一个常量，表示进行四舍五入的操作
		return b1.divide(b2, len, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	/**
	 * 当数值过大时进行转换，取大约数
	 * 
	 * @param d
	 * @return
	 */
	public static String subBigDecimal(double d) {
		DecimalFormat df = new DecimalFormat();
		df.applyPattern("0.000亿");
		return df.format(d);
	}

}
