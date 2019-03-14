package com.taobao.zeus.web.platform.server.rpc;

import java.util.Date;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.hb.zeus_sql_parse.Main;
import com.sencha.gxt.data.shared.loader.PagingLoadConfig;
import com.sencha.gxt.data.shared.loader.PagingLoadResult;
import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.JobDescriptorOld;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.JobStatus.TriggerType;
import com.taobao.zeus.util.Environment;
import com.taobao.zeus.util.Tuple;
import com.taobao.zeus.web.LoginUser;
import com.taobao.zeus.web.PermissionGroupManager;
import com.taobao.zeus.web.PermissionGroupManagerOld;
import com.taobao.zeus.web.platform.client.module.jobdisplay.job.JobHistoryModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModelAction;
import com.taobao.zeus.web.platform.client.util.GwtException;
import com.taobao.zeus.web.platform.client.util.HostGroupModel;
import com.taobao.zeus.web.platform.client.util.ZUser;
import com.taobao.zeus.web.platform.client.util.ZUserContactTuple;
import com.taobao.zeus.web.platform.shared.rpc.JobService;

public class FilterJobServiceImpl implements JobService{
	private static Logger log = LogManager.getLogger(FilterJobServiceImpl.class);
	
	@Autowired
	private PermissionGroupManagerOld permissionGroupManagerOld; 
	@Autowired
	private PermissionGroupManager permissionGroupManager;
	private JobService jobService;
	@Override
	public JobModel createJob(String jobName, String parentGroupId,
			String jobType) throws GwtException {
		if(Environment.isPrePub()){
			throw new GwtException("预发环境无法创建Job");
		}
		return jobService.createJob(jobName, parentGroupId, jobType);
	}

	@Override
	public void deleteJob(String jobId) throws GwtException {
		if(Environment.isPrePub()){
			throw new GwtException("预发环境无法删除Job");
		}
		jobService.deleteJob(jobId);
	}

	@Override
	public JobHistoryModel getJobHistory(String id) {
		return jobService.getJobHistory(id);
	}

	@Override
	public JobModel getJobStatus(String jobId) {
		return jobService.getJobStatus(jobId);
	}

	@Override
	public JobModel getUpstreamJob(String jobId) throws GwtException {
		return jobService.getUpstreamJob(jobId);
	}

	@Override
	public PagingLoadResult<JobHistoryModel> jobHistoryPaging(String jobId,PagingLoadConfig config) {
		return jobService.jobHistoryPaging(jobId,config);
	}

	@Override
	public void run(String jobId, int type) throws GwtException {
		TriggerType triggerType=null;
		if(type==1){
			triggerType=TriggerType.MANUAL;
		}else if(type==2){
			triggerType=TriggerType.MANUAL_RECOVER;
		}
		if(Environment.isPrePub() && triggerType==TriggerType.MANUAL_RECOVER){
			throw new GwtException("预发环境无法 执行 手动恢复操作");
		}
		jobService.run(jobId, type);
	}

	@Override
	public List<Long> switchAuto(String jobId, Boolean auto) throws GwtException {
		if(Environment.isPrePub()){
			throw new GwtException("预发环境无法修改状态");
		}
		return jobService.switchAuto(jobId, auto);
	}

	@Override
	public JobModel updateJob(JobModel jobModel) throws GwtException {
		if(Environment.isPrePub()){
			throw new GwtException("预发环境无法更新Job");
		}
		return jobService.updateJob(jobModel);
	}

	public void setJobService(JobService jobService) {
		this.jobService = jobService;
	}

	@Override
	public void addJobAdmin(String jobId, String uid) throws GwtException {
		jobService.addJobAdmin(jobId, uid);
	}

	@Override
	public List<ZUser> getJobAdmins(String jobId) {
		return jobService.getJobAdmins(jobId);
	}
	@Override
	public List<Long> getJobACtion(String jobId) {
		return jobService.getJobACtion(jobId);
	}
	@Override
	public void removeJobAdmin(String jobId, String uid) throws GwtException {
		jobService.removeJobAdmin(jobId, uid);
	}

	@Override
	public void transferOwner(String jobId, String uid) throws GwtException {
		jobService.transferOwner(jobId, uid);
	}

//	@Override
//	public List<JobHistoryModel> getRunningJobs(String groupId) {
//		return jobService.getRunningJobs(groupId);
//	}

	@Override
	public void cancel(String jobId) throws GwtException {
		jobService.cancel(jobId);
	}

//	@Override
//	public List<JobHistoryModel> getManualRunningJobs(String groupId) {
//		return jobService.getManualRunningJobs(groupId);
//	}

	@Override
	public List<JobHistoryModel> getAutoRunning(String groupId) {
		return jobService.getAutoRunning(groupId);
	}

	@Override
	public List<JobHistoryModel> getManualRunning(String groupId) {
		return jobService.getManualRunning(groupId);
	}

	@Override
	public void move(String jobId, String newGroupId) throws GwtException {
		jobService.move(jobId, newGroupId);
	}

	@Override
	public void syncScript(String jobId, String script) throws GwtException {
		jobService.syncScript(jobId, script);
	}

	@Override
	public PagingLoadResult<JobModelAction> getSubJobStatus(String groupId,
			PagingLoadConfig config, Date startDate, Date endDate) {
		return jobService.getSubJobStatus(groupId,config,startDate,endDate);
	}

	@Override
	public void grantImportantContact(String jobId, String uid)
			throws GwtException {
		jobService.grantImportantContact(jobId, uid);
		
	}

	@Override
	public void revokeImportantContact(String jobId, String uid)
			throws GwtException {
		jobService.revokeImportantContact(jobId, uid);
		
	}

	@Override
	public List<ZUserContactTuple> getAllContactList(String jobId) {
		return jobService.getAllContactList(jobId);
	}

	@Override
	public List<String> getJobDependencies(String jobId) throws GwtException {
		return jobService.getJobDependencies(jobId);
	}

	@Override
	public PagingLoadResult<HostGroupModel> getHostGroup(
			PagingLoadConfig config) {
		return jobService.getHostGroup(config);
	}

	@Override
	public void syncScriptAndHostGroupId(String jobId, String script,
			String hostGroupId) throws GwtException {
		jobService.syncScriptAndHostGroupId(jobId, script, hostGroupId);
	}

	@Override
	public String getHostGroupNameById(String hostGroupId) {
		return jobService.getHostGroupNameById(hostGroupId);
	}
	//自定义 新加函数
		@Override
		public String autoDependent(String jobId) {
			// TODO Auto-generated method stub
			Main m = new Main();
			String result = m.de_ids(jobId);
			return result;
		}
		
		// 自定义新增   多版本页面
		@Override
		public List<String> getJobVersion(String id) {
			 System.out.println("======Fileter================");
		    List<String> result = permissionGroupManager.getJobVersion(id);
           
			return result;
		}
		
		// 自定义新增   多版本页面
		@Override
		public JobModel updateVersion(String gmtModified,String id)throws GwtException{

			Tuple<JobDescriptorOld, JobStatus> t= null;
			try{
				t = permissionGroupManagerOld.updateVersion(LoginUser.getUser().getUid(), gmtModified, id);
			} catch (ZeusException e) {
				log.error(e);
				throw new GwtException(e.getMessage());
			}				
			JobDescriptorOld job = t.getX();
			return getUpstreamJob(job.getId());
		}
		
}