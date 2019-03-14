package com.hb.zeus_sql_parse;

import java.sql.SQLException;

/**
 * Hello world!
 *zues 依赖自动匹配
 */
public class Main 
{
	public static void main(String[] args) throws SQLException {
		//ParseSql ps = new ParseSql();
		
		//String sql ="select id,script,name from zeus_job where group_id in (22,15,16,14,13,27,25,34,36)";
		//String sql ="select id,script,name from zeus_job where group_id=60";
		//String sql = "select id,script,name from zeus_job where group_id in ("+args[0]+")";
		
		//String sql ="select id,script,name from zeus_job where id in ("+args[0]+")";
	   // String sql ="select id,script,name from zeus_job where id in (1804)";
		//ps.getSql(sql);
		Main m = new Main();
		System.out.println(m.de_ids("1807"));
	}
	
	public   String de_ids(String id) {
		ParseSql ps = new ParseSql();
		String sql ="select id,script,name from zeus_job where id="+id;
			
		return ps.getSql(sql);

	}
}
