package com.hb.zeus_sql_parse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Update {
	
	public static PreparedStatement  pst = null; 

	public static void main(String[] args) throws SQLException {
		Update u = new Update();
		
		u.update("update zeus_job set configs=?,cron_expression=?,dependencies=?,schedule_type=?  where id=?","6,7",5L);
	}
	public boolean update(String sql,String dependencies,Long id) throws SQLException{
		Connection conn = Mysql.getConnection();
		PreparedStatement ptmt = conn.prepareStatement(sql);
		ptmt.setString(1, "{\"roll.back.times\":\"3\",\"roll.back.wait.time\":\"10\",\"run.priority.level\":\"3\",\"zeus.dependency.cycle\":\"sameday\"}");
	    ptmt.setString(2, "");
	    ptmt.setString(3, dependencies);
	    ptmt.setInt(4, 1);
	    ptmt.setLong(5, id);
	    boolean r = ptmt.execute();
		Mysql.close(conn, pst, null);
		return r;
	}
}
