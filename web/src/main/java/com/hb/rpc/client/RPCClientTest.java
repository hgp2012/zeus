package com.hb.rpc.client;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jboss.netty.channel.Channel;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.JobStatus.Status;
import com.taobao.zeus.model.JobStatus.TriggerType;
import com.taobao.zeus.schedule.DistributeLocker;
import com.taobao.zeus.schedule.ZeusSchedule;
import com.taobao.zeus.socket.master.MasterContext;
import com.taobao.zeus.socket.master.MasterWorkerHolder;
import com.taobao.zeus.socket.protocol.Protocol.ExecuteKind;
import com.taobao.zeus.socket.worker.ClientWorker;
import com.taobao.zeus.store.JobHistoryManager;
import com.taobao.zeus.store.PermissionManager;
import com.taobao.zeus.store.mysql.persistence.JobPersistenceOld;
import com.taobao.zeus.util.Tuple;
import com.taobao.zeus.web.LoginUser;
import com.taobao.zeus.web.PermissionGroupManager;
import com.taobao.zeus.web.PermissionGroupManagerOld;
import com.taobao.zeus.web.platform.client.util.GwtException;
import com.taobao.zeus.web.platform.server.rpc.JobServiceImpl;
/**
 * 
 * 对外提供，手动执行任务的接口
 * @author hp 
 *
 */

public class RPCClientTest extends  HttpServlet  {
	
	private static final long serialVersionUID = 1L;
	private DistributeLocker locker;
	private ClientWorker clientWorker;
	private PermissionManager permissionManager;
	private PermissionGroupManager permissionGroupManager;
	private JobHistoryManager jobHistoryManager;
	private PermissionGroupManagerOld permissionGroupManagerOld;
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		ApplicationContext context = WebApplicationContextUtils
				.getWebApplicationContext(config.getServletContext());
		locker = (DistributeLocker) context.getBean("distributeLocker");
		clientWorker = (ClientWorker) context.getBean("clientWorker");
		
		permissionManager = (PermissionManager) context.getBean("permissionManager");
		permissionGroupManager = (PermissionGroupManager) context.getBean("permissionScheduleGroupManager");
		jobHistoryManager = (JobHistoryManager) context.getBean("jobHistoryManager");
		
		permissionGroupManagerOld = (PermissionGroupManagerOld) context.getBean("permissionScheduleGroupManagerOld");
		
	} 
	
	public void doGet(HttpServletRequest req, HttpServletResponse res){
		
        //业务逻辑
        try{  
        	if (locker != null) {
        		Field zeusScheduleField = locker.getClass().getDeclaredField(
						"zeusSchedule");
				zeusScheduleField.setAccessible(true);
				ZeusSchedule zeusSchedule = (ZeusSchedule) zeusScheduleField
						.get(locker);
				if (zeusSchedule != null) {
					Field masterContextField = zeusSchedule.getClass()
							.getDeclaredField("context");
					masterContextField.setAccessible(true);
					MasterContext context = (MasterContext) masterContextField
							.get(zeusSchedule);
					
					System.out.println("context============"+context);
					
					if (context != null) {
						String jobId = req.getParameter("op");
						int type = 1;  //1、手动执行，2手动恢复
						MasterWorkerHolder selectWorker=null;
						Map<Channel, MasterWorkerHolder> workers = context.getWorkers();
						for (Channel channel : workers.keySet()) {
							selectWorker = workers.get(channel);
						}
						System.out.println("workers====="+selectWorker.toString());
						System.out.println("op====="+jobId);						
						
						TriggerType triggerType = null;
						JobDescriptor jobDescriptor = null;
						ExecuteKind kind = null;

						List<String> job = permissionGroupManagerOld
								.getAllDependencied(jobId);
						
						System.out.println(job.toString());
						
						res.getWriter().println(job.toString());
						
					
					
						
					}
				}
        	}
        
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void doPost(HttpServletRequest req, HttpServletResponse res){
        this.doGet(req,res);    
    }
	


	

}
