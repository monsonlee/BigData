package store;

import domain.User;
import utils.JDBCUtil;

public class StoreImpl implements Store {

	public void store(User user) {
		String sql = "insert into user(userid,name,gender,location,business,employment,position,school,major,answersNum,starsNum,thxNum,saveNum,follow,follower) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		JDBCUtil.update(sql, user);
	}

}
