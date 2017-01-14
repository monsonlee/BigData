package download;

import domain.Page;
import utils.PageUtil;

public class DownLoadImpl implements DownLoad {

	/**
	 * 下载原始内容
	 */
	public Page download(String url) {
		Page page = new Page();
		page.setContent(PageUtil.getContent(url));
		page.setUrl(url);
		return page;
	}

}
