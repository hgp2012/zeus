package com.hb.zeus_sql_parse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ParseSql {
	
	public PreparedStatement pst = null; 
	static ResultSet ret = null;
	
	
	
	//获取sql语句
	public String getSql(String sql) {
		Connection conn = Mysql.getConnection();
		try {
			pst = conn.prepareStatement(sql);
			ret = pst.executeQuery();
			while (ret.next()) {  		
				String id = ret.getString(1);
	            String ress = ret.getString(2); 
	        	String name = ret.getString(3);
	            ArrayList<String> tables = parse(ress,name);
	            if(tables==null || tables.size()==0){
	            	continue;
	            }
	            removeDuplicate(tables);
	            List<String> ids = getIDs(tables);
	            
	          
	            String disable = ids.get(0);//没有匹配上的表
	            String m = ids.get(1);// 匹配上的表的ID
	            Long lid= Long.parseLong(id);
	            if(!disable.equals("")){
		            System.out.println("本任务ID====="+id);
		            System.out.println("没有匹配上的表====="+disable);
		            System.out.println("匹配上的ID===="+m);
		            System.out.println("--------------------------------------");
		            return disable;
	            }
	            else{
	            	System.out.println("本任务ID====="+id+"  匹配成功");
	            	Update u = new Update();
	            	boolean r = u.update("update zeus_job set configs=?,cron_expression=?,dependencies=?,schedule_type=?  where id=?", m, lid);
	            	return "success";
	            }          
			}
			Mysql.close(conn, pst, ret);
			return "No matches";
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "SQLException";
		} 
		
	}

	
	//获取sql语句
	public List<String> getIDs(List<String> tables) throws SQLException{
		if(tables == null) return null;
		
		Connection conn2 = Mysql.getConnection();
		StringBuffer disable= new StringBuffer();
		StringBuffer match = new StringBuffer();
		List<String> list = new ArrayList<String>();
		PreparedStatement pst2 = null;
		ResultSet ret2 = null;
		for(int i=0;i<tables.size();i++){
			String name = tables.get(i);
			 pst2 = conn2.prepareStatement("select id from zeus_job where name ='"+name+"'");//准备执行语句 
			 ret2 = pst2.executeQuery();
			String ids = null;
			while (ret2.next()) {  
	             ids = ret2.getString(1); 	             
			}
			if(ids == null || "".equals(ids)){
           	 disable.append(name).append(",");
            }else{
           	 match.append(ids).append(",");
            }
			
		}
		Mysql.close(conn2, pst2, ret2);
		list.add(disable.toString()); //没有匹配上的表
		if(match.toString().length()>0){
			list.add(match.toString().substring(0,match.toString().length()-1)); //匹配上的ID
		}else{
			list.add("");
		}
		
		return list;
	}
	
	// 获取所有的表
	public ArrayList<String> parse(String sql,String name){
		String s = sql.replaceAll("\n", " ").replaceAll("\\(", " ").replaceAll("\\)", " ")
				.replace("\\", " ").replace("'", " ").replace(":", " ").replace(",", " ")
				.replaceAll("\\+", " ").replaceAll("\\|", " ")
				 .replaceAll("\\{", " ").replaceAll("\\}", " ")
				 .replaceAll("\\(", " ").replaceAll("\\)", " ")
				 .replaceAll("\\^", " ").replaceAll("\\$", " ")
				 .replaceAll("\\[", " ").replaceAll("\\]", " ")
				 .replaceAll("\\?", " ").replaceAll("\\,", " ")
				 .replaceAll("\\&", " ").replaceAll("\"", " ");
		String[] words = s.split("\\s+");
		ArrayList<String> tables = new ArrayList<String>();
		for(int i=0;i<words.length;i++){
			
			if(matcher(words[i])){
				String w = words[i].trim().replaceAll(";", "");
				if(w.equals(name)) continue;//去掉自己
				tables.add(w);
			}
		}
		return tables;
	}
	
	//按照正则匹配出表名称
	public boolean matcher (String words){
		 String regEx = "houbank_(.*?)\\.(.*?)";
		 Pattern pattern = Pattern.compile(regEx);
		 Matcher matcher = pattern.matcher(words);
		 // 字符串是否与正则表达式相匹配
		 return  matcher.matches();
	}
	
	//删除list中重复的值
	public  void removeDuplicate(ArrayList<String> arlList){
		HashSet<String> h = new HashSet<String>(arlList);  
		arlList.clear();  
		arlList.addAll(h);  
	}

}
