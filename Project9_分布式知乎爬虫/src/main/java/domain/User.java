package domain;

/**
 * User用于存储用户信息
 *
 * @className User
 * @author mxlee
 * @email imxlee@foxmail.com
 */
public class User {

	String userID;// 用户ID

	String name;// 用户姓名

	String gender;// 性别

	String location;// 居住地;

	String business;// 行业

	String employment;// 公司

	String position;// 职位;

	String school;// 大学

	String major;// 专业

	int answersNum;// 回答数量

	int starsNum;// 被赞同数

	int thxNum;// 被感谢数

	int saveNum;// 被收藏数

	int follow;// 关注了

	int follower;// 关注者

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getBusiness() {
		return business;
	}

	public void setBusiness(String business) {
		this.business = business;
	}

	public String getEmployment() {
		return employment;
	}

	public void setEmployment(String employment) {
		this.employment = employment;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getSchool() {
		return school;
	}

	public void setSchool(String school) {
		this.school = school;
	}

	public String getMajor() {
		return major;
	}

	public void setMajor(String major) {
		this.major = major;
	}

	public int getAnswersNum() {
		return answersNum;
	}

	public void setAnswersNum(int answersNum) {
		this.answersNum = answersNum;
	}

	public int getStarsNum() {
		return starsNum;
	}

	public void setStarsNum(int starsNum) {
		this.starsNum = starsNum;
	}

	public int getThxNum() {
		return thxNum;
	}

	public void setThxNum(int thxNum) {
		this.thxNum = thxNum;
	}

	public int getSaveNum() {
		return saveNum;
	}

	public void setSaveNum(int saveNum) {
		this.saveNum = saveNum;
	}

	public int getFollow() {
		return follow;
	}

	public void setFollow(int follow) {
		this.follow = follow;
	}

	public int getFollower() {
		return follower;
	}

	public void setFollower(int follower) {
		this.follower = follower;
	}

	@Override
	public String toString() {
		return "用户ID：" + userID + "用户名：" + name + "\n性别：" + gender + "\n居住地：" + location + "\n行业：" + business + "\n公司："
				+ employment + "\n职位：" + position + "\n大学：" + school + "\n专业：" + major + "\n回答数：" + answersNum
				+ "\n关注数：" + follow + "\n被关注数：" + follower + "\n被点赞数：" + starsNum + "\n被感谢数：" + thxNum + "\n被收藏数："
				+ saveNum;
	}

}
