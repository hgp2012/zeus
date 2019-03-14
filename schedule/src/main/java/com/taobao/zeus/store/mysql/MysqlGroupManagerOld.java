package com.taobao.zeus.store.mysql;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.orm.hibernate3.HibernateCallback;
import org.springframework.orm.hibernate3.HibernateTemplate;
import org.springframework.orm.hibernate3.support.HibernateDaoSupport;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptorOld;
import com.taobao.zeus.model.JobDescriptorOld.JobRunTypeOld;
import com.taobao.zeus.model.JobDescriptorOld.JobScheduleTypeOld;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.processer.DownloadProcesser;
import com.taobao.zeus.model.processer.Processer;
import com.taobao.zeus.socket.master.Master;
import com.taobao.zeus.store.GroupBeanOld;
import com.taobao.zeus.store.GroupManagerOld;
import com.taobao.zeus.store.GroupManagerToolOld;
import com.taobao.zeus.store.JobBeanOld;
import com.taobao.zeus.store.mysql.persistence.GroupPersistence;
import com.taobao.zeus.store.mysql.persistence.JobPersistence;
import com.taobao.zeus.store.mysql.persistence.JobPersistenceOld;
import com.taobao.zeus.store.mysql.persistence.JobPersistenceOldVersion;
import com.taobao.zeus.store.mysql.persistence.Worker;
import com.taobao.zeus.store.mysql.persistence.ZeusUser;
import com.taobao.zeus.store.mysql.tool.GroupValidate;
import com.taobao.zeus.store.mysql.tool.JobValidateOld;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvertOld;
import com.taobao.zeus.util.CronExpParser;
import com.taobao.zeus.util.Tuple;
import com.taobao.zeus.util.ZeusDateTool;

@SuppressWarnings("unchecked")
public class MysqlGroupManagerOld extends HibernateDaoSupport implements GroupManagerOld {
	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		GroupBeanOld group = getDownstreamGroupBean(groupId);
		if (group.isDirectory()) {
			// if (!group.getChildrenGroupBeans().isEmpty()) {
			// throw new ZeusException("该组下不为空，无法删除");
			// }
			boolean candelete = true;
			for (GroupBeanOld child : group.getChildrenGroupBeans()) {
				if (child.isExisted()) {
					candelete = false;
					break;
				}
			}
			if (!candelete) {
				throw new ZeusException("该组下不为空，无法删除");
			}
		} else {
			if (!group.getJobBeans().isEmpty()) {
				throw new ZeusException("该组下不为空，无法删除");
			}
		}
		GroupPersistence object = (GroupPersistence) getHibernateTemplate().get(GroupPersistence.class,
				Integer.valueOf(groupId));
		object.setExisted(0);
		object.setGmtModified(new Date());
		getHibernateTemplate().update(object);
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		GroupBeanOld root = getGlobeGroupBean();
		JobBeanOld job = root.getAllSubJobBeans().get(jobId);
		if (!job.getDepender().isEmpty()) {
			List<String> deps = new ArrayList<String>();
			for (JobBeanOld jb : job.getDepender()) {
				deps.add(jb.getJobDescriptor().getId());
			}
			throw new ZeusException("该Job正在被其他Job" + deps.toString() + "依赖，无法删除");
		}
		getHibernateTemplate().delete(getHibernateTemplate().get(JobPersistenceOld.class, Long.valueOf(jobId)));
	}

	@Override
	public GroupBeanOld getDownstreamGroupBean(String groupId) {
		GroupDescriptor group = getGroupDescriptor(groupId);
		GroupBeanOld result = new GroupBeanOld(group);
		return getDownstreamGroupBean(result);
	}

	@Override
	public GroupBeanOld getDownstreamGroupBean(GroupBeanOld parent) {
		if (parent.isDirectory()) {
			List<GroupDescriptor> children = getChildrenGroup(parent.getGroupDescriptor().getId());
			for (GroupDescriptor child : children) {
				GroupBeanOld childBean = new GroupBeanOld(child);
				getDownstreamGroupBean(childBean);
				childBean.setParentGroupBean(parent);
				parent.getChildrenGroupBeans().add(childBean);
			}
		} else {
			List<Tuple<JobDescriptorOld, JobStatus>> jobs = getChildrenJob(parent.getGroupDescriptor().getId());
			for (Tuple<JobDescriptorOld, JobStatus> tuple : jobs) {
				JobBeanOld JobBeanOld = new JobBeanOld(tuple.getX(), tuple.getY());
				JobBeanOld.setGroupBean(parent);
				parent.getJobBeans().put(tuple.getX().getId(), JobBeanOld);
			}
		}

		return parent;
	}

	@Override
	public GroupBeanOld getGlobeGroupBean() {
		return GroupManagerToolOld.buildGlobeGroupBean(this);
	}

	/**
	 * 获取叶子组下所有的Job
	 * 
	 * @param groupId
	 * @return
	 */
	@Override
	public List<Tuple<JobDescriptorOld, JobStatus>> getChildrenJob(String groupId) {
		List<JobPersistenceOld> list = getHibernateTemplate()
				.find("from com.taobao.zeus.store.mysql.persistence.JobPersistenceOld where groupId=" + groupId);
		List<Tuple<JobDescriptorOld, JobStatus>> result = new ArrayList<Tuple<JobDescriptorOld, JobStatus>>();
		if (list != null) {
			for (JobPersistenceOld j : list) {
				result.add(PersistenceAndBeanConvertOld.convert(j));
			}
		}
		return result;
	}

	/**
	 * 获取组的下级组列表
	 * 
	 * @param groupId
	 * @return
	 */
	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		List<GroupPersistence> list = getHibernateTemplate()
				.find("from com.taobao.zeus.store.mysql.persistence.GroupPersistence where parent=" + groupId);
		List<GroupDescriptor> result = new ArrayList<GroupDescriptor>();
		if (list != null) {
			for (GroupPersistence p : list) {
				result.add(PersistenceAndBeanConvertOld.convert(p));
			}
		}
		return result;
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		GroupPersistence persist = (GroupPersistence) getHibernateTemplate().get(GroupPersistence.class,
				Integer.valueOf(groupId));
		if (persist != null) {
			return PersistenceAndBeanConvertOld.convert(persist);
		}
		return null;
	}

	@Override
	public Tuple<JobDescriptorOld, JobStatus> getJobDescriptor(String jobId) {
		JobPersistenceOld persist = getJobPersistence(jobId);
		if (persist == null) {
			return null;
		}
		Tuple<JobDescriptorOld, JobStatus> t = PersistenceAndBeanConvertOld.convert(persist);
		JobDescriptorOld jd = t.getX();
		// 如果是周期任务，并且依赖不为空，则需要封装周期任务的依赖
		if (jd.getScheduleType() == JobScheduleTypeOld.CyleJob && jd.getDependencies() != null) {
			JobPersistenceOld jp = null;
			for (String jobID : jd.getDependencies()) {
				if (StringUtils.isNotEmpty(jobID)) {
					jp = getJobPersistence(jobID);
					if (jp != null) {
						jd.getDepdCycleJob().put(jobID, jp.getCycle());
					}
				}
			}

		}
		return t;
	}

	private JobPersistenceOld getJobPersistence(String jobId) {
		JobPersistenceOld persist = (JobPersistenceOld) getHibernateTemplate().get(JobPersistenceOld.class,
				Long.valueOf(jobId));
		if (persist == null) {
			return null;
		}
		return persist;
	}
	
	//自定义新增  多版本页面  数据修改 ，查询出页面指定版本的数据，替换job表的数据
	@Override
	public Tuple<JobDescriptorOld, JobStatus> updateVersion(String user, final String gmtModified,final String id) {
		return (Tuple<JobDescriptorOld, JobStatus>) getHibernateTemplate().execute(new HibernateCallback() {

			@Override
			public Object doInHibernate(Session session) throws HibernateException, SQLException {
				Query query = session.createQuery(
						"from com.taobao.zeus.store.mysql.persistence.JobPersistenceOldVersion where  gmtModified='"+gmtModified+"' and jobId="+id);
				query.setMaxResults(1);
				List<JobPersistenceOldVersion> list = query.list();
				if (list == null || list.size() == 0) {
					return null;
				}
				JobPersistenceOldVersion jv = list.get(0);
				JobPersistenceOld job = new JobPersistenceOld();
				job.setAuto(jv.getAuto());
				job.setConfigs(jv.getConfigs());
				job.setCronExpression(jv.getCronExpression());
				job.setCycle(jv.getCycle());
				job.setDependencies(jv.getDependencies());
				job.setDescr(jv.getDescr());
				job.setGmtCreate(jv.getGmtCreate());
				job.setGmtModified(jv.getGmtModified());
				job.setGroupId(jv.getGroupId());
				job.setHistoryId(jv.getHistoryId());
				job.setHost(jv.getHost());
				job.setHostGroupId(jv.getHostGroupId());
				job.setId(jv.getJobId());
				job.setLastEndTime(jv.getLastEndTime());
				job.setLastResult(jv.getLastResult());
				job.setName(jv.getName());
				job.setOffset(jv.getOffset());
				job.setOwner(jv.getOwner());
				job.setPostProcessers(jv.getPostProcessers());
				job.setPreProcessers(jv.getPreProcessers());
				job.setReadyDependency(jv.getReadyDependency());
				job.setResources(jv.getResources());
				job.setRunType(jv.getRunType());
				job.setScheduleType(jv.getScheduleType());
				job.setScript(jv.getScript());
				job.setStartTime(jv.getStartTime());
				job.setStartTimestamp(jv.getStartTimestamp());
				job.setStatisEndTime(jv.getStatisEndTime());
				job.setStatisStartTime(jv.getStatisStartTime());
				job.setStatus(jv.getStatus());
				job.setTimezone(jv.getTimezone());
				
				getHibernateTemplate().update(job);
				Tuple<JobDescriptorOld, JobStatus> t = PersistenceAndBeanConvertOld.convert(job);
				return t;
			}
		});
	}

	@Override
	public String getRootGroupId() {
		return (String) getHibernateTemplate().execute(new HibernateCallback() {

			@Override
			public Object doInHibernate(Session session) throws HibernateException, SQLException {
				Query query = session.createQuery(
						"from com.taobao.zeus.store.mysql.persistence.GroupPersistence g order by g.id asc");
				query.setMaxResults(1);
				List<GroupPersistence> list = query.list();
				if (list == null || list.size() == 0) {
					GroupPersistence persist = new GroupPersistence();
					persist.setName("众神之神");
					persist.setOwner(ZeusUser.ADMIN.getUid());
					persist.setDirectory(0);
					session.save(persist);
					if (persist.getId() == null) {
						return null;
					}
					return String.valueOf(persist.getId());
				}
				return String.valueOf(list.get(0).getId());
			}
		});
	}

	@Override
	public GroupBeanOld getUpstreamGroupBean(String groupId) {
		return GroupManagerToolOld.getUpstreamGroupBean(groupId, this);
	}

	@Override
	public JobBeanOld getUpstreamJobBean(String jobId) {
		return GroupManagerToolOld.getUpstreamJobBean(jobId, this);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group) throws ZeusException {
		GroupPersistence old = (GroupPersistence) getHibernateTemplate().get(GroupPersistence.class,
				Integer.valueOf(group.getId()));
		updateGroup(user, group, old.getOwner(), old.getParent() == null ? null : old.getParent().toString());
	}

	public void updateGroup(String user, GroupDescriptor group, String owner, String parent) throws ZeusException {

		GroupPersistence old = (GroupPersistence) getHibernateTemplate().get(GroupPersistence.class,
				Integer.valueOf(group.getId()));

		GroupPersistence persist = PersistenceAndBeanConvertOld.convert(group);

		persist.setOwner(owner);
		if (parent != null) {
			persist.setParent(Integer.valueOf(parent));
		}

		// 以下属性不允许修改，强制采用老的数据
		persist.setDirectory(old.getDirectory());
		persist.setGmtCreate(old.getGmtCreate());
		persist.setGmtModified(new Date());
		persist.setExisted(old.getExisted());

		getHibernateTemplate().update(persist);
	}

	@Override
	public void updateJob(String user, JobDescriptorOld job) throws ZeusException {
		JobPersistenceOld orgPersist = (JobPersistenceOld) getHibernateTemplate().get(JobPersistenceOld.class,
				Long.valueOf(job.getId()));
		updateJob(user, job, orgPersist.getOwner(), orgPersist.getGroupId().toString());
	}

	public void updateJob(String user, JobDescriptorOld job, String owner, String groupId) throws ZeusException {
		JobPersistenceOld orgPersist = (JobPersistenceOld) getHibernateTemplate().get(JobPersistenceOld.class,
				Long.valueOf(job.getId()));
		if (job.getScheduleType() == JobScheduleTypeOld.Independent) {
			job.setDependencies(new ArrayList<String>());
		} else if (job.getScheduleType() == JobScheduleTypeOld.Dependent) {
			job.setCronExpression("");
		}
		job.setOwner(owner);
		job.setGroupId(groupId);
		// 以下属性不允许修改，强制采用老的数据
		JobPersistenceOld persist = PersistenceAndBeanConvertOld.convert(job);
		persist.setGmtCreate(orgPersist.getGmtCreate());
		persist.setGmtModified(new Date());
		persist.setRunType(orgPersist.getRunType());
		persist.setStatus(orgPersist.getStatus());
		persist.setReadyDependency(orgPersist.getReadyDependency());
		persist.setHost(job.getHost());
		persist.setHostGroupId(Integer.valueOf(job.getHostGroupId()));
		// 如果是用户从界面上更新，开始时间、统计周期等均为空，用原来的值
		if (job.getStartTime() == null || "".equals(job.getStartTime())) {
			persist.setStartTime(orgPersist.getStartTime());
		}
		if (job.getStartTimestamp() == 0) {
			persist.setStartTimestamp(orgPersist.getStartTimestamp());
		}
		if (job.getStatisStartTime() == null || "".equals(job.getStatisStartTime())) {
			persist.setStatisStartTime(orgPersist.getStatisStartTime());
		}
		if (job.getStatisEndTime() == null || "".equals(job.getStatisEndTime())) {
			persist.setStatisEndTime(orgPersist.getStatisEndTime());
		}

		// 如果是周期任务，则许检查依赖周期是否正确
		if (JobScheduleTypeOld.CyleJob.equals(job.getScheduleType()) && job.getDependencies() != null
				&& job.getDependencies().size() != 0) {
			List<JobDescriptorOld> list = this.getJobDescriptors(job.getDependencies());
			jobValidateOld.checkCycleJob(job, list);
		}

		if (jobValidateOld.valide(job)) {
			getHibernateTemplate().update(persist);
		}
		
		//---自定义修改，增加多版本功能，写job表的同时，也写一份到job的多版本表---
		JobPersistenceOldVersion persistVersion = PersistenceAndBeanConvertOld.convert2(job);
		persistVersion.setJobId(persist.getId());
		getHibernateTemplate().save(persistVersion);
		//-------------自定义修改结束-----------------------

	}

	@Autowired
	private JobValidateOld jobValidateOld;

	@Override
	public GroupDescriptor createGroup(String user, String groupName, String parentGroup, boolean isDirectory)
			throws ZeusException {
		if (parentGroup == null) {
			throw new ZeusException("parent group may not be null");
		}
		GroupDescriptor group = new GroupDescriptor();
		group.setOwner(user);
		group.setName(groupName);
		group.setParent(parentGroup);
		group.setDirectory(isDirectory);

		GroupValidate.valide(group);

		GroupPersistence persist = PersistenceAndBeanConvertOld.convert(group);
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		persist.setExisted(1);
		getHibernateTemplate().save(persist);
		return PersistenceAndBeanConvertOld.convert(persist);
	}
	

	

	@Override
	public JobDescriptorOld createJob(String user, String jobName, String parentGroup, JobRunTypeOld jobType)
			throws ZeusException {
		GroupDescriptor parent = getGroupDescriptor(parentGroup);
		if (parent.isDirectory()) {
			throw new ZeusException("目录组下不得创建Job");
		}
		JobDescriptorOld job = new JobDescriptorOld();
		job.setOwner(user);
		job.setName(jobName);
		job.setGroupId(parentGroup);
		job.setJobType(jobType);
		job.setPreProcessers(Arrays.asList((Processer) new DownloadProcesser()));
		JobPersistenceOld persist = PersistenceAndBeanConvertOld.convert(job);
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		getHibernateTemplate().save(persist);
		
//		//自定义修改，增加多版本功能，写job表的同时，也写一份到job的多版本表
//		JobPersistenceOldVersion persistVersion = PersistenceAndBeanConvertOld.convert2(job);
//		persistVersion.setJobId(persist.getId());
//		System.out.println("jobid======"+persist.getId());
//		getHibernateTemplate().save(persistVersion);
//		//-------------自定义修改结束-----------------------

		// 自定义修改，增加写action表的操作，新增任务的时候就可以手动执行
		persist.setScheduleType(0);
		persist.setCronExpression("0 0 3 * * ?");
		persist.setHost("0");
		Date now = new Date();
		SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat df3 = new SimpleDateFormat("yyyyMMddHHmmss");
		String currentDateStr = df3.format(now) + "0000";
		List<JobPersistenceOld> jobDetails = new ArrayList<JobPersistenceOld>();
		jobDetails.add(persist);
		Map<Long, JobPersistence> actionDetails = new HashMap<Long, JobPersistence>();
		runScheduleJobToAction(jobDetails, now, df2, actionDetails, currentDateStr);
		// ------------自定义修改结束--------------------------------------

		return PersistenceAndBeanConvertOld.convert(persist).getX();
	}

	//自定义 新加 写action表方法  新加任务是，生成一条action表数据，这样就可以马上手动执行，测试任务
	public void runScheduleJobToAction(List<JobPersistenceOld> jobDetails, Date now, SimpleDateFormat dfDate, Map<Long, JobPersistence> actionDetails, String currentDateStr){
		for(JobPersistenceOld jobDetail : jobDetails){
			//ScheduleType: 0 独立任务; 1依赖任务; 2周期任务
			if(jobDetail.getScheduleType() != null && jobDetail.getScheduleType()==0){
				try{
					String jobCronExpression = jobDetail.getCronExpression();
					String cronDate= dfDate.format(now);
					List<String> lTime = new ArrayList<String>();
					if(jobCronExpression != null && jobCronExpression.trim().length() > 0){
						
						//定时调度
						boolean isCronExp = false;
						try{
							isCronExp = CronExpParser.Parser(jobCronExpression, cronDate, lTime);
						}catch(Exception ex){
							isCronExp = false;
						}
						if (!isCronExp) {
							System.out.println("无法生成Cron表达式：日期," + cronDate + ";不符合规则cron表达式：");
						}
						for (int i = 0; i < lTime.size(); i++) {
							String actionDateStr = ZeusDateTool.StringToDateStr(lTime.get(i), "yyyy-MM-dd HH:mm:ss", "yyyyMMddHHmm");
							String actionCronExpr = ZeusDateTool.StringToDateStr(lTime.get(i), "yyyy-MM-dd HH:mm:ss", "0 m H d M") + " ?";
							
							JobPersistence actionPer = new JobPersistence();
							actionPer.setId(Long.parseLong(actionDateStr)*1000000+jobDetail.getId());//update action id
							actionPer.setToJobId(jobDetail.getId());
							actionPer.setAuto(jobDetail.getAuto());
							actionPer.setConfigs(jobDetail.getConfigs());
							actionPer.setCronExpression(actionCronExpr);//update action cron expression
							actionPer.setCycle(jobDetail.getCycle());
							String jobDependencies = jobDetail.getDependencies();
							actionPer.setDependencies(jobDependencies);
							actionPer.setJobDependencies(jobDependencies);
							actionPer.setDescr(jobDetail.getDescr());
							actionPer.setGmtCreate(jobDetail.getGmtCreate());
							actionPer.setGmtModified(new Date());
							actionPer.setGroupId(jobDetail.getGroupId());
							actionPer.setHistoryId(jobDetail.getHistoryId());
							actionPer.setHost("0");
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
/*							actionPer.setScript(jobDetail.getScript());*/
							actionPer.setStartTime(jobDetail.getStartTime());
							actionPer.setStartTimestamp(jobDetail.getStartTimestamp());
							actionPer.setStatisStartTime(jobDetail.getStatisStartTime());
							actionPer.setStatisEndTime(jobDetail.getStatisEndTime());
							actionPer.setStatus(jobDetail.getStatus());
							actionPer.setTimezone(jobDetail.getTimezone());
							try {
								//System.out.println("定时任务JobId: " + jobDetail.getId()+";  ActionId: " +actionPer.getId());
								System.out.println("定时任务JobId: " + jobDetail.getId()+";  ActionId: " +actionPer.getId());
								//if(actionPer.getId()>Long.parseLong(currentDateStr)){
			
								getHibernateTemplate().saveOrUpdate(actionPer);	
									//System.out.println("success");
									System.out.println("success");
									actionDetails.put(actionPer.getId(),actionPer);
								//}
							} catch (Exception e) {
							//	System.out.println("failed");
								System.out.println("定时任务JobId:" + jobDetail.getId() + " 生成Action" +actionPer.getId() + "失败");
							}
						}
					}
				}catch(Exception ex){
					//log.error("定时任务生成Action失败",ex);
					System.out.println("定时任务生成Action失败"+ex);
				}
			}
	}
}

	@Override
	public Map<String, Tuple<JobDescriptorOld, JobStatus>> getJobDescriptor(final Collection<String> jobIds) {
		List<Tuple<JobDescriptorOld, JobStatus>> list = (List<Tuple<JobDescriptorOld, JobStatus>>) getHibernateTemplate()
				.execute(new HibernateCallback() {

					@Override
					public Object doInHibernate(Session session) throws HibernateException, SQLException {
						if (jobIds.isEmpty()) {
							return Collections.emptyList();
						}
						List<Long> ids = new ArrayList<Long>();
						for (String i : jobIds) {
							ids.add(Long.valueOf(i));
						}
						Query query = session.createQuery(
								"from com.taobao.zeus.store.mysql.persistence.JobPersistenceOld where id in (:list)");
						query.setParameterList("list", ids);
						List<JobPersistenceOld> list = query.list();
						List<Tuple<JobDescriptorOld, JobStatus>> result = new ArrayList<Tuple<JobDescriptorOld, JobStatus>>();
						if (list != null && !list.isEmpty()) {
							for (JobPersistenceOld persist : list) {
								result.add(PersistenceAndBeanConvertOld.convert(persist));
							}
						}
						return result;
					}
				});

		Map<String, Tuple<JobDescriptorOld, JobStatus>> map = new HashMap<String, Tuple<JobDescriptorOld, JobStatus>>();
		for (Tuple<JobDescriptorOld, JobStatus> jd : list) {
			map.put(jd.getX().getId(), jd);
		}
		return map;
	}

	public List<JobDescriptorOld> getJobDescriptors(final Collection<String> jobIds) {
		List<JobDescriptorOld> list = (List<JobDescriptorOld>) getHibernateTemplate().execute(new HibernateCallback() {
			@Override
			public Object doInHibernate(Session session) throws HibernateException, SQLException {
				if (jobIds.isEmpty()) {
					return Collections.emptyList();
				}
				List<Long> ids = new ArrayList<Long>();
				for (String i : jobIds) {
					if (StringUtils.isNotEmpty(i)) {
						ids.add(Long.valueOf(i));
					}
				}
				if (ids.isEmpty()) {
					return Collections.emptyList();
				}
				Query query = session.createQuery(
						"from com.taobao.zeus.store.mysql.persistence.JobPersistenceOld where id in (:list)");
				query.setParameterList("list", ids);
				List<JobPersistenceOld> list = query.list();
				List<JobDescriptorOld> result = new ArrayList<JobDescriptorOld>();
				if (list != null && !list.isEmpty()) {
					for (JobPersistenceOld persist : list) {
						result.add(PersistenceAndBeanConvertOld.convert(persist).getX());
					}
				}
				return result;
			}
		});
		return list;
	}
	

	/**
	 * 自定义新增    指定任务及依赖任务执行   查询本次要执行的所有任务
	 * @param jobIds
	 * @return
	 */
	public List<JobPersistenceOld> getJobDescriptors2(final Collection<String> jobIds) {
		List<JobPersistenceOld> list = (List<JobPersistenceOld>) getHibernateTemplate().execute(new HibernateCallback() {
			@Override
			public Object doInHibernate(Session session) throws HibernateException, SQLException {
				if (jobIds.isEmpty()) {
					return Collections.emptyList();
				}
				List<Long> ids = new ArrayList<Long>();
				for (String i : jobIds) {
					if (StringUtils.isNotEmpty(i)) {
						ids.add(Long.valueOf(i));
					}
				}
				if (ids.isEmpty()) {
					return Collections.emptyList();
				}
				Query query = session.createQuery(
						"from com.taobao.zeus.store.mysql.persistence.JobPersistenceOld where id in (:list)");
				query.setParameterList("list", ids);
				List<JobPersistenceOld> list = query.list();
				
				return list;
			}
		});
		return list;
	}

	@Override
	public void updateJobStatus(JobStatus jobStatus) {
		JobPersistenceOld persistence = getJobPersistence(jobStatus.getJobId());
		persistence.setGmtModified(new Date());

		// 只修改状态 和 依赖 2个字段
		JobPersistenceOld temp = PersistenceAndBeanConvertOld.convert(jobStatus);
		persistence.setStatus(temp.getStatus());
		persistence.setReadyDependency(temp.getReadyDependency());
		persistence.setHistoryId(temp.getHistoryId());

		getHibernateTemplate().update(persistence);
	}

	@Override
	public JobStatus getJobStatus(String jobId) {
		Tuple<JobDescriptorOld, JobStatus> tuple = getJobDescriptor(jobId);
		if (tuple == null) {
			return null;
		}
		return tuple.getY();
	}

	@Override
	public void grantGroupOwner(String granter, String uid, String groupId) throws ZeusException {
		GroupDescriptor gd = getGroupDescriptor(groupId);
		if (gd != null) {
			updateGroup(granter, gd, uid, gd.getParent());
		}
	}

	@Override
	public void grantJobOwner(String granter, String uid, String jobId) throws ZeusException {
		Tuple<JobDescriptorOld, JobStatus> job = getJobDescriptor(jobId);
		if (job != null) {
			job.getX().setOwner(uid);
			updateJob(granter, job.getX(), uid, job.getX().getGroupId());
		}
	}

	@Override
	public void moveJob(String uid, String jobId, String groupId) throws ZeusException {
		JobDescriptorOld jd = getJobDescriptor(jobId).getX();
		GroupDescriptor gd = getGroupDescriptor(groupId);
		if (gd.isDirectory()) {
			throw new ZeusException("非法操作");
		}
		updateJob(uid, jd, jd.getOwner(), groupId);
	}

	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId) throws ZeusException {
		GroupDescriptor gd = getGroupDescriptor(groupId);
		GroupDescriptor parent = getGroupDescriptor(newParentGroupId);
		if (!parent.isDirectory()) {
			throw new ZeusException("非法操作");
		}
		updateGroup(uid, gd, gd.getOwner(), newParentGroupId);
	}

	@Override
	public List<String> getHosts() throws ZeusException {
		return (List<String>) getHibernateTemplate().execute(new HibernateCallback() {
			public Object doInHibernate(Session session) throws HibernateException, SQLException {
				Query query = session.createQuery("select host from com.taobao.zeus.store.mysql.persistence.Worker");
				return query.list();
			}
		});
	}

	@Override
	public void replaceWorker(Worker worker) throws ZeusException {
		try {
			getHibernateTemplate().saveOrUpdate(worker);
		} catch (DataAccessException e) {
			throw new ZeusException(e);
		}

	}

	@Override
	public void removeWorker(String host) throws ZeusException {
		try {
			getHibernateTemplate().delete(getHibernateTemplate().get(Worker.class, host));
		} catch (DataAccessException e) {
			throw new ZeusException(e);
		}

	}

	/**
	 * 获取All Jobs
	 * 
	 * @param groupId
	 * @return
	 */
	@Override
	public List<JobPersistenceOld> getAllJobs() {
		List<JobPersistenceOld> list = getHibernateTemplate()
				.find("from com.taobao.zeus.store.mysql.persistence.JobPersistenceOld ");
		return list;
	}

	@Override
	public List<String> getAllDependencied(String jobID) {
		List<JobPersistenceOld> jobs = getAllJobs();
		if (jobs == null || jobs.size() == 0)
			return null;
		Map<String, List<String>> allJobDependencied = new HashMap<String, List<String>>();
		for (JobPersistenceOld job : jobs) {
			JobDescriptorOld jobd = PersistenceAndBeanConvertOld.convert(job).getX();
			if (jobd != null && jobd.hasDependencies()) {
				List<String> deps = jobd.getDependencies();
				for (String dep : deps) {
					List<String> depds = allJobDependencied.get(dep);
					if (depds == null) {
						depds = new ArrayList<String>();
					}
					depds.add(job.getId().toString());
					allJobDependencied.put(dep, depds);
				}
			}
		}

		List<String> dependencied = new ArrayList<String>();
		Set<String> visited = new HashSet<String>();
		Queue<String> idQueue = new LinkedList<String>();
		idQueue.offer(jobID);
		visited.add(jobID);
		while (!idQueue.isEmpty()) {
			String id = idQueue.poll();
			List<String> depdList = allJobDependencied.get(id);
			if (depdList != null && depdList.size() != 0) {
				for (String depd : depdList) {
					if (!visited.contains(depd)) {
						visited.add(depd);
						idQueue.offer(depd);
						dependencied.add(depd);
					}
				}
			}
		}
		return dependencied;
	}

	@Override
	public List<String> getAllDependencies(String jobID) {
		JobDescriptorOld job = getJobDescriptor(jobID).getX();
		if (job == null || !job.hasDependencies())
			return null;
		List<String> dependencies = new ArrayList<String>();
		Set<String> visited = new HashSet<String>();
		Queue<String> idQueue = new LinkedList<String>();
		idQueue.offer(jobID);
		visited.add(jobID);
		while (!idQueue.isEmpty()) {
			String id = idQueue.poll();
			JobDescriptorOld jb = getJobDescriptor(id).getX();
			if (jb != null && jb.hasDependencies()) {
				List<String> deps = jb.getDependencies();
				if (deps != null && deps.size() != 0) {
					for (String dep : deps) {
						if (!visited.contains(dep)) {
							visited.add(dep);
							idQueue.offer(dep);
							dependencies.add(dep);
						}
					}
				}
			}
		}
		return dependencies;
	}

	@Override
	public void updateActionList(JobDescriptorOld job) {
		JobPersistenceOld persist = PersistenceAndBeanConvertOld.convert(job);
		Long jobId = persist.getId();
		/* String script = persist.getScript(); */
		String resources = persist.getResources();
		String configs = persist.getConfigs();
		String host = persist.getHost();
		Integer workGroupId = persist.getHostGroupId();
		Integer auto = persist.getAuto();
		logger.info("begin updateActionList.");
		HibernateTemplate template = getHibernateTemplate();
		List<JobPersistence> actionList = template
				.find("from com.taobao.zeus.store.mysql.persistence.JobPersistence where toJobId='" + jobId
						+ "' order by id desc");
		logger.info("finish query.");
		if (actionList != null && actionList.size() > 0) {
			for (JobPersistence actionPer : actionList) {
				// if(!"running".equalsIgnoreCase(actionPer.getStatus())){
				/* actionPer.setScript(script); */
				actionPer.setResources(resources);
				actionPer.setConfigs(configs);
				actionPer.setHost("0");
				actionPer.setGmtModified(new Date());
				actionPer.setHostGroupId(workGroupId);
				actionPer.setAuto(auto);
				template.saveOrUpdate(actionPer);
				// }
			}
			logger.info("finish update " + actionList.size() + ".");
		}
	}


}