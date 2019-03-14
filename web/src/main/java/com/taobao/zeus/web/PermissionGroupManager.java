package com.taobao.zeus.web;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;
import com.taobao.zeus.model.JobDescriptorOld;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.processer.JobProcesser;
import com.taobao.zeus.model.processer.Processer;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.GroupManager;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.PermissionManager;
import com.taobao.zeus.store.UserManager;
import com.taobao.zeus.store.mysql.persistence.JobPersistence;
import com.taobao.zeus.store.mysql.persistence.JobPersistenceOld;
import com.taobao.zeus.store.mysql.persistence.Worker;
import com.taobao.zeus.store.mysql.persistence.ZeusUser;
import com.taobao.zeus.util.CronExpParser;
import com.taobao.zeus.util.DateUtil;
import com.taobao.zeus.util.Tuple;
import com.taobao.zeus.util.ZeusDateTool;
import com.taobao.zeus.util.ZeusStringUtil;

/**
 * 权限验证，需要的操作权限验证不通过，将抛出异常
 * 
 * @author zhoufang
 *
 */
public class PermissionGroupManager implements GroupManager {

	private Logger log = LogManager.getLogger(PermissionGroupManager.class);
	private GroupManager groupManager;

	public void setGroupManager(GroupManager groupManager) {
		this.groupManager = groupManager;
	}

	@Autowired
	@Qualifier("permissionManager")
	private PermissionManager permissionManager;
	@Autowired
	@Qualifier("userManager")
	private UserManager userManager;

	private Boolean isGroupOwner(String uid, GroupBean gb) {
		List<String> owners = new ArrayList<String>();
		while (gb != null) {
			if (!owners.contains(gb.getGroupDescriptor().getOwner())) {
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb = gb.getParentGroupBean();
		}
		if (owners.contains(uid)) {
			return true;
		}
		return false;
	}

	private Boolean isGroupOwner(String uid, String groupId) {
		return isGroupOwner(uid, groupManager.getUpstreamGroupBean(groupId));
	}

	// 自定义修改。
	private Boolean isJobOwner(String uid, String jobId) {
		JobBean jb = groupManager.getUpstreamJobBean(jobId);
		if (jb.getJobDescriptor().getOwner().equalsIgnoreCase(uid)) {
			return true;
		}
		// return isGroupOwner(uid, jb.getGroupBean());
		return false;
	}

	// 自定义修改。
	public Boolean hasGroupPermission(String uid, String groupId) {
		if (isGroupOwner(uid, groupId)) {
			return true;
		}
		// return permissionManager.hasGroupPermission(uid, groupId);
		return false;
	}

	// 自定义修改。
	public Boolean hasJobPermission(String uid, String jobId) {
		if (isJobOwner(uid, jobId)) {
			return true;
		}
		// return permissionManager.hasJobPermission(uid, jobId);
		return false;
	}

	// 自定义修改
	@Override
	public GroupDescriptor createGroup(String user, String groupName, String parentGroup, boolean isDirectory)
			throws ZeusException {
		// if(hasGroupPermission(user, parentGroup)){
		return groupManager.createGroup(user, groupName, parentGroup, isDirectory);
		// }else{
		// throw new ZeusException("您无权操作");
		// }
	}

	// 自定义修改
	@Override
	public JobDescriptor createJob(String user, String jobName, String parentGroup, JobRunType jobType)
			throws ZeusException {
		// if(hasGroupPermission(user, parentGroup)){
		return groupManager.createJob(user, jobName, parentGroup, jobType);
		// }else{
		// throw new ZeusException("您无权操作");
		// }
	}

	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		if (hasGroupPermission(user, groupId)) {
			GroupDescriptor gd = groupManager.getGroupDescriptor(groupId);
			if (gd != null && gd.getOwner().equals(user)) {
				groupManager.deleteGroup(user, groupId);
			}
		} else {
			throw new ZeusException("您无权操作");
		}

	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		if (hasJobPermission(user, jobId)) {
			Tuple<JobDescriptor, JobStatus> job = groupManager.getJobDescriptor(jobId);
			if (job != null) {
				groupManager.deleteJob(user, jobId);
			}
		} else {
			throw new ZeusException("没有删除的权限");
		}
	}

	@Override
	public GroupBean getDownstreamGroupBean(String groupId) {
		return groupManager.getDownstreamGroupBean(groupId);
	}

	@Override
	public GroupBean getGlobeGroupBean() {
		return groupManager.getGlobeGroupBean();
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		return groupManager.getGroupDescriptor(groupId);
	}

	@Override
	public Tuple<JobDescriptor, JobStatus> getJobDescriptor(String jobId) {
		return groupManager.getJobDescriptor(jobId);
	}

	@Override
	public String getRootGroupId() {
		return groupManager.getRootGroupId();
	}

	@Override
	public GroupBean getUpstreamGroupBean(String groupId) {
		return groupManager.getUpstreamGroupBean(groupId);
	}

	@Override
	public JobBean getUpstreamJobBean(String jobId) {
		return groupManager.getUpstreamJobBean(jobId);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group) throws ZeusException {
		if (hasGroupPermission(user, group.getId())) {
			GroupDescriptor gd = groupManager.getGroupDescriptor(group.getId());
			if (gd != null) {
				groupManager.updateGroup(user, group);
			}
		} else {
			throw new ZeusException("没有更新的权限");
		}

	}

	@Override
	public void updateJob(String user, JobDescriptor job) throws ZeusException {
		if (hasJobPermission(user, job.getId())) {
			Tuple<JobDescriptor, JobStatus> old = groupManager.getJobDescriptor(job.getId());
			if (old != null) {
				List<JobProcesser> hasadd = new ArrayList<JobProcesser>();
				for (Processer p : old.getX().getPreProcessers()) {
					if (p instanceof JobProcesser) {
						hasadd.add((JobProcesser) p);
					}
				}
				for (Processer p : old.getX().getPostProcessers()) {
					if (p instanceof JobProcesser) {
						hasadd.add((JobProcesser) p);
					}
				}
				List<JobProcesser> thistime = new ArrayList<JobProcesser>();
				for (Processer p : job.getPreProcessers()) {
					if (p instanceof JobProcesser) {
						thistime.add((JobProcesser) p);
					}
				}
				for (Processer p : job.getPostProcessers()) {
					if (p instanceof JobProcesser) {
						thistime.add((JobProcesser) p);
					}
				}
				for (JobProcesser jp : thistime) {
					if (jp.getJobId().equals(job.getId())) {
						throw new ZeusException("不得将自身设置为自身的处理器");
					}
					boolean exist = false;
					for (JobProcesser jp2 : hasadd) {
						if (jp2.getId().equalsIgnoreCase(jp.getId())) {
							exist = true;
							break;
						}
					}
					if (!exist && !hasJobPermission(user, jp.getJobId())) {
						throw new ZeusException("您没有权限将Job：" + jp.getJobId() + " 添加到处理单元中");
					}
				}
				groupManager.updateJob(user, job);
			}
		} else {
			throw new ZeusException("没有更新的权限");
		}

	}

	@Override
	public Map<String, Tuple<JobDescriptor, JobStatus>> getJobDescriptor(Collection<String> jobIds) {
		return groupManager.getJobDescriptor(jobIds);
	}

	@Override
	public void updateJobStatus(JobStatus jobStatus) {
		throw new UnsupportedOperationException("PermissionGroupManager 不支持此操作");
	}

	@Override
	public JobStatus getJobStatus(String jobId) {
		return groupManager.getJobStatus(jobId);
	}

	@Override
	public void grantGroupOwner(String granter, String uid, String groupId) throws ZeusException {
		GroupBean gb = groupManager.getUpstreamGroupBean(groupId);
		List<String> owners = new ArrayList<String>();
		while (gb != null) {
			if (!owners.contains(gb.getGroupDescriptor().getOwner())) {
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb = gb.getParentGroupBean();
		}
		if (owners.contains(granter)) {
			groupManager.grantGroupOwner(granter, uid, groupId);
		} else {
			throw new ZeusException("您无权操作");
		}
	}

	@Override
	public void grantJobOwner(String granter, String uid, String jobId) throws ZeusException {
		JobBean jb = groupManager.getUpstreamJobBean(jobId);
		List<String> owners = new ArrayList<String>();
		owners.add(jb.getJobDescriptor().getOwner());
		GroupBean gb = jb.getGroupBean();
		while (gb != null) {
			if (!owners.contains(gb.getGroupDescriptor().getOwner())) {
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb = gb.getParentGroupBean();
		}
		if (owners.contains(granter)) {
			groupManager.grantJobOwner(granter, uid, jobId);
		} else {
			throw new ZeusException("您无权操作");
		}

	}

	public void addGroupAdmin(String granter, String user, String groupId) throws ZeusException {
		if (isGroupOwner(granter, groupId)) {
			permissionManager.addGroupAdmin(user, groupId);
		} else {
			throw new ZeusException("您无权操作");
		}
	}

	public void addJobAdmin(String granter, String user, String jobId) throws ZeusException {
		if (isJobOwner(granter, jobId)) {
			permissionManager.addJobAdmin(user, jobId);
		} else {
			throw new ZeusException("您无权操作");
		}
	}

	public void removeGroupAdmin(String granter, String user, String groupId) throws ZeusException {
		if (isGroupOwner(granter, groupId)) {
			permissionManager.removeGroupAdmin(user, groupId);
		} else {
			throw new ZeusException("您无权操作");
		}
	}

	public void removeJobAdmin(String granter, String user, String jobId) throws ZeusException {
		if (isJobOwner(granter, jobId)) {
			permissionManager.removeJobAdmin(user, jobId);
		} else {
			throw new ZeusException("您无权操作");
		}
	}

	public List<ZeusUser> getGroupAdmins(String groupId) {
		return userManager.findListByUid(permissionManager.getGroupAdmins(groupId));
	}

	public List<ZeusUser> getJobAdmins(String jobId) {
		return userManager.findListByUid(permissionManager.getJobAdmins(jobId));
	}

	public List<Long> getJobACtion(String jobId) {
		return permissionManager.getJobACtion(jobId);
	}

	// 自定义新增 多版本页面
	public List<String> getJobVersion(String jobId) {
		return permissionManager.getJobVersion(jobId);
	}

	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		return groupManager.getChildrenGroup(groupId);
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getChildrenJob(String groupId) {
		return groupManager.getChildrenJob(groupId);
	}

	@Override
	public GroupBean getDownstreamGroupBean(GroupBean parent) {
		return groupManager.getDownstreamGroupBean(parent);
	}

	@Override
	public void moveJob(String uid, String jobId, String groupId) throws ZeusException {
		if (!permissionManager.hasGroupPermission(uid, groupId) || !permissionManager.hasJobPermission(uid, jobId)) {
			throw new ZeusException("您无权操作");
		}
		groupManager.moveJob(uid, jobId, groupId);
	}

	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId) throws ZeusException {
		if (!permissionManager.hasGroupPermission(uid, groupId)
				|| !permissionManager.hasGroupPermission(uid, newParentGroupId)) {
			throw new ZeusException("您无权操作");
		}
		groupManager.moveGroup(uid, groupId, newParentGroupId);
	}

	@Override
	public List<String> getHosts() throws ZeusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void replaceWorker(Worker worker) throws ZeusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeWorker(String host) throws ZeusException {
		// TODO Auto-generated method stub

	}

	@Override
	public void saveJob(JobPersistence actionPer) throws ZeusException {
		groupManager.saveJob(actionPer);

	}

	@Override
	public List<JobPersistence> getLastJobAction(String jobId) {
		return groupManager.getLastJobAction(jobId);
	}

	@Override
	public void updateAction(JobDescriptor actionPer) throws ZeusException {
		groupManager.updateAction(actionPer);
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getActionList(String jobId) {
		return groupManager.getActionList(jobId);
	}

	@Override
	public void removeJob(Long actionId) throws ZeusException {
		groupManager.removeJob(actionId);

	}

	@Override
	public boolean IsExistedBelowRootGroup(String GroupName) {
		return groupManager.IsExistedBelowRootGroup(GroupName);
	}

	/**
	 * 自定义新增 指定任务及依赖任务执行 生成依赖任务的action信息
	 * 
	 * @param jobDetails
	 * @param actionDetails
	 * @param currentDateStr
	 */
	public void runDependencesJobToAction(List<JobPersistenceOld> jobDetails, Long id_pre, List<String> dependences,
			Map<Long, JobPersistence> actionDetails, String currentDateStr) {
		for (JobPersistenceOld jobDetail : jobDetails) {
			// ScheduleType: 0 独立任务; 1依赖任务; 2周期任务
			if ((jobDetail.getScheduleType() != null && jobDetail.getScheduleType() == 1)
					|| (jobDetail.getScheduleType() != null && jobDetail.getScheduleType() == 2)) {
				try {
					String jobDependencies = jobDetail.getDependencies();
					System.out.println("==================" + jobDetail.getScheduleType());
					// 计算依赖测actionId
					String actionDependencies = "";
					String subJobDependencies = "";
					if (jobDependencies != null && jobDependencies.trim().length() > 0) {
						String[] dependStrs = jobDependencies.split(",");
						for (int i = 0; i < dependStrs.length; i++) {
							if (dependences.contains(dependStrs[i])) {
								Long actionId = id_pre + Long.parseLong(dependStrs[i]);
								actionDependencies = actionId + ",";
								subJobDependencies = dependStrs[i] + ",";
							}
						}
						actionDependencies = actionDependencies.substring(0, actionDependencies.length() - 1);
						subJobDependencies = subJobDependencies.substring(0, subJobDependencies.length() - 1);
						// 保存多版本的action
						JobPersistence actionPer = new JobPersistence();

						actionPer.setId(id_pre + jobDetail.getId());// update
																	// action
																	// id
						actionPer.setToJobId(jobDetail.getId());
						actionPer.setAuto(jobDetail.getAuto());
						actionPer.setConfigs(jobDetail.getConfigs());
						actionPer.setCronExpression(jobDetail.getCronExpression());// update
						actionPer.setCycle(jobDetail.getCycle());
						actionPer.setDependencies(actionDependencies);
						actionPer.setJobDependencies(subJobDependencies);
						actionPer.setDescr(jobDetail.getDescr());
						actionPer.setGmtCreate(jobDetail.getGmtCreate());
						actionPer.setGmtModified(new Date());
						actionPer.setGroupId(jobDetail.getGroupId());
						actionPer.setHistoryId(jobDetail.getHistoryId());
						actionPer.setHost(jobDetail.getHost());
						actionPer.setHostGroupId(jobDetail.getHostGroupId());
						actionPer.setLastEndTime(jobDetail.getLastEndTime());
						actionPer.setLastResult(jobDetail.getLastResult());
						actionPer.setName(jobDetail.getName());
						actionPer.setOffset(jobDetail.getOffset());
						actionPer.setOwner(jobDetail.getOwner());
						actionPer.setPostProcessers(jobDetail.getPostProcessers());
						actionPer.setPreProcessers(jobDetail.getPreProcessers());
						actionPer.setReadyDependency(jobDetail.getReadyDependency());
						actionPer.setResources(jobDetail.getResources());
						actionPer.setRunType(jobDetail.getRunType());
						actionPer.setScheduleType(jobDetail.getScheduleType());
						/* actionPer.setScript(jobDetail.getScript()); */
						actionPer.setStartTime(jobDetail.getStartTime());
						actionPer.setStartTimestamp(jobDetail.getStartTimestamp());
						actionPer.setStatisStartTime(jobDetail.getStatisStartTime());
						actionPer.setStatisEndTime(jobDetail.getStatisEndTime());
						actionPer.setStatus(jobDetail.getStatus());
						actionPer.setTimezone(jobDetail.getTimezone());
						System.out.println("======dep====saveJob== start===");
						try {
							if (!actionDetails.containsKey(actionPer.getId())) {
								// System.out.println("依赖任务JobId: " +
								// jobDetail.getId()+"; ActionId: "
								// +actionPer.getId());
								// log.info("依赖任务JobId: " + jobDetail.getId()+";
								// ActionId: " +actionPer.getId());
								// if(actionPer.getId()>Long.parseLong(currentDateStr)){
								System.out.println("======dep====saveJob=====");
								groupManager.saveJob(actionPer);
								System.out.println("success");
								// log.info("success");
								// }
								actionDetails.put(actionPer.getId(), actionPer);

							}
						} catch (ZeusException e) {
							log.error("依赖任务JobId:" + jobDetail.getId() + " 生成Action" + actionPer.getId() + "失败", e);
						}
					}
				} catch (Exception e) {
					log.error("依赖任务生成失败");
				}

			}

		}
	}

	/**
	 * 自定义新增 指定任务及依赖任务执行 生成定时任务的action信息
	 * 
	 * @param jobDetails
	 * @param now
	 * @param dfDate
	 * @param actionDetails
	 * @param currentDateStr
	 */
	public void runScheduleJobToAction(List<JobPersistenceOld> jobDetails, Long id, Date now, SimpleDateFormat dfDate,
			Map<Long, JobPersistence> actionDetails, String currentDateStr) {
		System.out.println("======runScheduleJobToAction=============");
		for (JobPersistenceOld jobDetail : jobDetails) {
			// ScheduleType: 0 独立任务; 1依赖任务; 2周期任务
			jobDetail.setScheduleType(0);
			System.out.println("======runScheduleJobToAction=============" + jobDetail.getScheduleType());
			if (jobDetail.getScheduleType() != null && jobDetail.getScheduleType() == 0) {
				try {

					JobPersistence actionPer = new JobPersistence();
					actionPer.setId(id);// update
										// action
										// id
					actionPer.setToJobId(jobDetail.getId());
					actionPer.setAuto(jobDetail.getAuto());
					actionPer.setConfigs(
							"{\"roll.back.times\":\"3\",\"roll.back.wait.time\":\"10\",\"run.priority.level\":\"3\"}");
					actionPer.setCronExpression("0 0 3 * * ?");// update
																// action
																// cron
																// expression
					actionPer.setCycle(jobDetail.getCycle());
					// String jobDependencies = jobDetail.getDependencies();
					// actionPer.setDependencies(jobDependencies);
					// actionPer.setJobDependencies(jobDependencies);
					actionPer.setDependencies("");
					actionPer.setJobDependencies("");
					actionPer.setDescr(jobDetail.getDescr());
					actionPer.setGmtCreate(jobDetail.getGmtCreate());
					actionPer.setGmtModified(new Date());
					actionPer.setGroupId(jobDetail.getGroupId());
					actionPer.setHistoryId(jobDetail.getHistoryId());
					actionPer.setHost(jobDetail.getHost());
					actionPer.setHostGroupId(jobDetail.getHostGroupId());
					actionPer.setLastEndTime(jobDetail.getLastEndTime());
					actionPer.setLastResult(jobDetail.getLastResult());
					actionPer.setName(jobDetail.getName());
					actionPer.setOffset(jobDetail.getOffset());
					actionPer.setOwner(jobDetail.getOwner());
					actionPer.setPostProcessers(jobDetail.getPostProcessers());
					actionPer.setPreProcessers(jobDetail.getPreProcessers());
					actionPer.setReadyDependency(jobDetail.getReadyDependency());
					actionPer.setResources(jobDetail.getResources());
					actionPer.setRunType(jobDetail.getRunType());
					actionPer.setScheduleType(jobDetail.getScheduleType());
					/* actionPer.setScript(jobDetail.getScript()); */
					actionPer.setStartTime(jobDetail.getStartTime());
					actionPer.setStartTimestamp(jobDetail.getStartTimestamp());
					actionPer.setStatisStartTime(jobDetail.getStatisStartTime());
					actionPer.setStatisEndTime(jobDetail.getStatisEndTime());
					actionPer.setStatus(jobDetail.getStatus());
					actionPer.setTimezone(jobDetail.getTimezone());
					try {
						// System.out.println("定时任务JobId: " +
						// jobDetail.getId()+"; ActionId: "
						// +actionPer.getId());
						log.info("定时任务JobId: " + jobDetail.getId() + ";  ActionId: " + actionPer.getId());
						System.out.println("定时任务JobId: " + jobDetail.getId() + ";  ActionId: " + actionPer.getId());
						// if(actionPer.getId()>Long.parseLong(currentDateStr)){
						groupManager.saveJob(actionPer);
						System.out.println("success");
						log.info("success");
						actionDetails.put(actionPer.getId(), actionPer);
						// }
					} catch (ZeusException e) {
						System.out.println("failed");
						log.error("定时任务JobId:" + jobDetail.getId() + " 生成Action" + actionPer.getId() + "失败", e);
					}

				} catch (Exception ex) {
					log.error("定时任务生成Action失败", ex);
					// System.out.println("定时任务生成Action失败"+ex);
				}
			}
		}
	}

	@Override
	public List<JobPersistence> getJobPersistences(Collection<String> jobIds) {
		// TODO Auto-generated method stub
		return null;
	}
}
