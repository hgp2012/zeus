package com.hb.rpc.client;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jboss.netty.channel.Channel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptorOld;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.JobDescriptorOld.JobRunTypeOld;
import com.taobao.zeus.model.JobDescriptorOld.JobScheduleTypeOld;
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
import com.taobao.zeus.store.mysql.persistence.JobPersistence;
import com.taobao.zeus.store.mysql.persistence.JobPersistenceOld;
import com.taobao.zeus.util.DateUtil;
import com.taobao.zeus.util.Environment;
import com.taobao.zeus.util.Tuple;
import com.taobao.zeus.util.ZeusStringUtil;
import com.taobao.zeus.web.LoginUser;
import com.taobao.zeus.web.PermissionGroupManager;
import com.taobao.zeus.web.PermissionGroupManagerOld;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModel;
import com.taobao.zeus.web.platform.client.util.GwtException;
import com.taobao.zeus.web.platform.server.rpc.JobServiceImpl;
/**
 * 
 * 对外提供，手动执行任务的接口
 * @author hp
 *
 */

public class RPCClient extends  HttpServlet  {
	
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
					
					
					/**
					 * flat 标识
					 * 1、手动执行、手动恢复、手动依赖
					 * 2、新增
					 * 3、修改
					 * 4、删除
					 * 
					 */
					String flat = req.getParameter("flat");
					
					System.out.println("falt====="+flat);
					if(flat.equals("1")){  //1、手动执行、手动恢复、手动依赖
						if (context != null) {
							String jobId = req.getParameter("op");
							int type = 1;  //1、手动执行，2手动恢复，3手动依赖
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
							if (type == 1) {
								triggerType = TriggerType.MANUAL;
								kind = ExecuteKind.ManualKind;
							} else if (type == 2) {
								triggerType = TriggerType.MANUAL_RECOVER;
								kind = ExecuteKind.ScheduleKind;
							}else if(type == 3){
								triggerType = TriggerType.MANUAL_DEPENDENT;
								kind = ExecuteKind.ScheduleKind;
								//查询出jobId的所有依赖任务
								List<String> dependencieds = permissionGroupManagerOld.getAllDependencied(jobId);
								dependencieds.add(jobId);
								/**
								 * 先生成一套action表信息
								 * action表的ID与自动生成的有差异
								 * 
								 */
								
								Date now = new Date();
								SimpleDateFormat df2 = new SimpleDateFormat(
										"yyyy-MM-dd");
								SimpleDateFormat df3 = new SimpleDateFormat(
										"yyyyMMddHHmmss");
								String currentDateStr = df3.format(now) + "0000";
								Map<Long, JobPersistence> actionDetails = new HashMap<Long, JobPersistence>();
								//生成第一个任务的action
								List<String> list = new ArrayList<String>();
								list.add(jobId);
								//生成action表id编号
								System.out.println("========jobId===1111============="+jobId);
								 String actionPreStr = DateUtil.getToday2().concat(ZeusStringUtil.getRandomNum());
								 Long actionPre = (Long.parseLong(actionPreStr)/ 100000)* 100000; // actionID前缀
								 Long id = actionPre + Long.parseLong(jobId);
								 jobId = String.valueOf(id);
								 System.out.println("======rn================"+actionPre);
								 System.out.println("========jobId================"+jobId);
								List<JobPersistenceOld> jobDetails1 = permissionGroupManagerOld.getJobDescriptors2(list);
								System.out.println("========jobDetails1================"+jobDetails1.toString());
								permissionGroupManager.runScheduleJobToAction(jobDetails1,id ,now, df2, actionDetails,
										currentDateStr);
								//生成其他依赖任务的action
								List<JobPersistenceOld> jobDetails2 = permissionGroupManagerOld.getJobDescriptors2(dependencieds);
								permissionGroupManager.runDependencesJobToAction(jobDetails2,actionPre,dependencieds, actionDetails, currentDateStr);
							}
							if (!permissionManager.hasActionPermission(
									"biadmin", jobId)) {
								GwtException e = new GwtException("你没有权限执行该操作");
								//log.error(e);
								throw e;
							}
							Tuple<JobDescriptor, JobStatus> job = permissionGroupManager
									.getJobDescriptor(jobId);
							jobDescriptor = job.getX();
							JobHistory history = new JobHistory();
							history.setJobId(jobId);
							history.setToJobId(jobDescriptor.getToJobId());
							history.setTriggerType(triggerType);
//							history.setOperator(LoginUser.getUser().getUid());
							history.setOperator(jobDescriptor.getOwner());
							history.setIllustrate("触发人：biadmin" );
							history.setStatus(Status.RUNNING);
							history.setStatisEndTime(jobDescriptor.getStatisEndTime());
							history.setTimezone(jobDescriptor.getTimezone());
//							history.setExecuteHost(jobDescriptor.getHost());
							history.setHostGroupId(jobDescriptor.getHostGroupId());
							jobHistoryManager.addJobHistory(history);

							try {
								clientWorker.executeJobFromWeb(kind, history.getId());
							} catch (Exception e) {
								//log.error("error", e);
								throw new GwtException(e.getMessage());
							}
						
							//clientWorker.executeJobFromWeb(ExecuteKind.ManualKind, op);
							
						}
					}else if(flat.equals("2")){//新增任务
						
						String jobType=req.getParameter("jobType");
						String jobName=req.getParameter("jobName");
						String parentGroupId=req.getParameter("parentGroupId");
						
						System.out.println(jobType+"   "+jobName+"  "+parentGroupId);
								
						JobRunTypeOld type = null;
						JobModel model = new JobModel();
						if ("mapreduce".equals(jobType)) {
							type = JobRunTypeOld.MapReduce;
						} else if ("shell".equals(jobType)) {
							type = JobRunTypeOld.Shell;
						} else if ("hive".equals(jobType)) {
							type = JobRunTypeOld.Hive;
						}
						try {
							System.out.println("=====write start===========");
							JobDescriptorOld jd = permissionGroupManagerOld.createJob("biadmin", jobName, parentGroupId, type);
							System.out.println("=====write end===========");
							//model = getUpstreamJob(jd.getId());
							//model.setDefaultTZ(DateUtil.getDefaultTZStr());
						} catch (ZeusException e) {
							throw new GwtException(e.getMessage());
						}
					
					}else if(flat.equals("3")){//修改任务
						
					}
					else if (flat.equals("4")){ // 删除任务
						try {
							String jobId=req.getParameter("jobId");
							permissionGroupManagerOld.deleteJob("biadmin",
									jobId);
						} catch (ZeusException e) {
							throw new GwtException(e.getMessage());
						}
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
	
    public void updateJob(JobModel jobModel)  {
		JobDescriptorOld jd = new JobDescriptorOld();
		jd.setCronExpression(jobModel.getCronExpression());
		jd.setDependencies(jobModel.getDependencies());
		jd.setDesc(jobModel.getDesc());
		jd.setGroupId(jobModel.getGroupId());
		jd.setId(jobModel.getId());
		JobRunTypeOld type = null;
		if (jobModel.getJobRunType().equals(JobModel.MapReduce)) {
			type = JobRunTypeOld.MapReduce;
		} else if (jobModel.getJobRunType().equals(JobModel.SHELL)) {
			type = JobRunTypeOld.Shell;
		} else if (jobModel.getJobRunType().equals(JobModel.HIVE)) {
			type = JobRunTypeOld.Hive;
		}
		jd.setJobType(type);
		JobScheduleTypeOld scheduleType = null;
		if (JobModel.DEPEND_JOB.equals(jobModel.getJobScheduleType())) {
			scheduleType = JobScheduleTypeOld.Dependent;
		}
		if (JobModel.INDEPEN_JOB.equals(jobModel.getJobScheduleType())) {
			scheduleType = JobScheduleTypeOld.Independent;
		}
		if (JobModel.CYCLE_JOB.equals(jobModel.getJobScheduleType())) {
			scheduleType = JobScheduleTypeOld.CyleJob;
		}
		jd.setName(jobModel.getName());
		jd.setOwner(jobModel.getOwner());
		jd.setResources(jobModel.getLocalResources());
		jd.setProperties(jobModel.getLocalProperties());
		jd.setScheduleType(scheduleType);
		jd.setScript(jobModel.getScript());
		jd.setAuto(jobModel.getAuto());

		//jd.setPreProcessers(parseProcessers(jobModel.getPreProcessers()));
		//jd.setPostProcessers(parseProcessers(jobModel.getPostProcessers()));
		jd.setTimezone(jobModel.getDefaultTZ());
		jd.setOffRaw(jobModel.getOffRaw());
		jd.setCycle(jobModel.getJobCycle());
		jd.setHost(jobModel.getHost());
		if (jobModel.getHostGroupId() == null) {
			jd.setHostGroupId(Environment.getDefaultWorkerGroupId());
		}else {
			jd.setHostGroupId(jobModel.getHostGroupId());
		}
		try {
			permissionGroupManagerOld.updateJob(LoginUser.getUser().getUid(),
					jd);
		
		} catch (ZeusException e) {
			System.out.println(e.getMessage());
		}
	}

	

}
