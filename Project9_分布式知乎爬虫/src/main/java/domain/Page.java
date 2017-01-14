package domain;

public class Page {
	/**
	 * 下载原始内容
	 */
	private String content;
	/**
	 * URL
	 */
	private String url;


	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

}
