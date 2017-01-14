package utils;
public class TestFilter {
	public static void main(String[] args) {
		BloomFilter b = new BloomFilter();
		b.addValue("www.baidu.com");
		b.addValue("www.sohu.com");

		System.out.println(b.contains("www.baid.com"));
		System.out.println(b.contains("www.sohu.com"));
//		String md5 = MD5Filter.md5("www.github.com");
//		String md5_1 = MD5Filter.md5("www.github.com");
//		System.out.println(md5);
//		System.out.println(md5_1);
	}
}
