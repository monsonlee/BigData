package utils;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PageUtil {
	private static Logger logger = LoggerFactory.getLogger(PageUtil.class.getSimpleName());

	/**
	 * 下载原始内容
	 */
	public static String getContent(String url) {
		String content = null;
		CloseableHttpClient client = null;
		CloseableHttpResponse response = null;
		try {
			long startTime = System.currentTimeMillis();
			HttpClientBuilder builder = HttpClients.custom();// HttpClient构建器
			PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();// 连接池
			cm.setMaxTotal(200);// 最大连接数
			cm.setDefaultMaxPerRoute(20);// 最大路由连接数
			builder.setConnectionManager(cm);
			// 设置超时
			final int retryTime = 3;
			RequestConfig defaultRequestConfig = RequestConfig.custom().setSocketTimeout(5000).setConnectTimeout(5000)
					.setConnectionRequestTimeout(5000).build();
			builder.setDefaultRequestConfig(defaultRequestConfig);
			// 设置重试次数
			builder.setRetryHandler(new HttpRequestRetryHandler() {
				public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
					if (executionCount >= retryTime) {
						return false;
					}
					return true;
				}
			});
			// 获取一个HttpClient对象，模拟浏览器
			client = builder.build();
			HttpUriRequest request = new HttpGet(url);
			request.setHeader("User-Agent",
					"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.75 Safari/537.36");
			response = client.execute(request);// 执行请求
			// 获取一个Entity实体对象
			HttpEntity entity = response.getEntity();
			content = EntityUtils.toString(entity);// 原始内容
			long endTime = System.currentTimeMillis();
			logger.info("页面下载成功,url:{},消耗时间:{}", url, endTime - startTime);
		} catch (Exception e) {
			logger.error("页面下载失败,url:{}", url);
		} finally {
			try {
				if (response != null) {
					response.close();
				}
				if (client != null) {
					client.close();
				}
			} catch (IOException e) {
				logger.error("资源释放失败" + e.getMessage());
			}
		}
		return content;
	}

}
