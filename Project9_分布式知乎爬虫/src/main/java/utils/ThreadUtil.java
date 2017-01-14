package utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtil {
	static Logger logger = LoggerFactory.getLogger(ThreadUtil.class.getSimpleName());

	public static void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
		}
	}

}
