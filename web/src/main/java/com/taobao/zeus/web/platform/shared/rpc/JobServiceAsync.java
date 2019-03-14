package com.taobao.zeus.web.platform.shared.rpc;

import java.util.Date;
import java.util.List;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.sencha.gxt.data.shared.loader.PagingLoadConfig;
import com.sencha.gxt.data.shared.loader.PagingLoadResult;
import com.taobao.zeus.web.platform.client.module.jobdisplay.job.JobHistoryModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModelAction;
import com.taobao.zeus.web.platform.client.util.HostGroupModel;
import com.taobao.zeus.web.platform.client.util.ZUser;
import com.taobao.zeus.web.platform.client.util.ZUserContactTuple;
import com.taobao.zeus.web.platform.client.util.async.AbstractAsyncCallback;

public interface JobServiceAsync {

	void addJobAdmin(String jobId, String uid, AsyncCallback<Void> callback);

	void cancel(String jobId, AsyncCallback<Void> callback);

	void createJob(String jobName, String parentGroupId, String jobType,
			AsyncCallback<JobModel> callback);

	void deleteJob(String jobId, AsyncCallback<Void> callback);

	void getAutoRunning(String groupId,
			AsyncCallback<List<JobHistoryModel>> callback);

	void getJobAdmins(String jobId, AsyncCallback<List<ZUser>> callback);

	void getJobHistory(String id, AsyncCallback<JobHistoryModel> callback);

	void getJobStatus(String jobId, AsyncCallback<JobModel> callback);

	void getManualRunning(String groupId,
			AsyncCallback<List<JobHistoryModel>> callback);

//	void getManualRunningJobs(String groupId,
//			AsyncCallback<List<JobHistoryModel>> callback);

//	void getRunningJobs(String groupId,
//			AsyncCallback<List<JobHistoryModel>> callback);
	
/*	void getSubJobStatusAction(String groupId, PagingLoadConfig config,
			AsyncCallback<PagingLoadResult<JobModelAction>> callback);*/

	void getUpstreamJob(String jobId, AsyncCallback<JobModel> callback);

	void jobHistoryPaging(String jobId, PagingLoadConfig config,
			AsyncCallback<PagingLoadResult<JobHistoryModel>> callback);

	void removeJobAdmin(String jobId, String uid, AsyncCallback<Void> callback);

	void run(String jobId, int type, AsyncCallback<Void> callback);

	void switchAuto(String jobId, Boolean auto, AsyncCallback<List<Long>> callback);

	void transferOwner(String jobId, String uid, AsyncCallback<Void> callback);

	void updateJob(JobModel jobModel, AsyncCallback<JobModel> callback);

	void move(String jobId, String newGroupId, AsyncCallback<Void> callback);

	void syncScript(String jobId, String script, AsyncCallback<Void> callback);

	void getJobACtion(String id,
			AsyncCallback<List<Long>> callback);

	void getSubJobStatus(String groupId, PagingLoadConfig config, Date startDate,
			Date endDate, AsyncCallback<PagingLoadResult<JobModelAction>> callback);

	void grantImportantContact(String jobId, String uid,
			AsyncCallback<Void> callback);

	void revokeImportantContact(String jobId, String uid,
			AsyncCallback<Void> callback);

	void getAllContactList(String jobId,
			AsyncCallback<List<ZUserContactTuple>> callback);

	void getJobDependencies(String jobId, AsyncCallback<List<String>> callback);

	void getHostGroup(PagingLoadConfig config,
			AsyncCallback<PagingLoadResult<HostGroupModel>> callback);

	void syncScriptAndHostGroupId(String jobId, String script,
			String hostGroupId, AsyncCallback<Void> callback);

	void getHostGroupNameById(String hostGroupId,
			AsyncCallback<String> callback);

	// 自定义新增   自动匹配依赖
	void autoDependent(String id, AsyncCallback<String> callback); 
	
	//自定义  多版本页面
	void getJobVersion(String id, AsyncCallback<List<String>> callback);
	
	//自定义  多版本页面
	void updateVersion(String gmtModified,String id,AsyncCallback<JobModel> callback);
	
//	void getHostGroupNameById(String hostGroupId, AsyncCallback<String> callback);
}