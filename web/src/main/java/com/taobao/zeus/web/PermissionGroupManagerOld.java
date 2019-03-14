package com.taobao.zeus.web;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptorOld;
import com.taobao.zeus.model.JobDescriptorOld.JobRunTypeOld;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.processer.JobProcesser;
import com.taobao.zeus.model.processer.Processer;
import com.taobao.zeus.store.GroupBeanOld;
import com.taobao.zeus.store.GroupManagerOld;
import com.taobao.zeus.store.JobBeanOld;
import com.taobao.zeus.store.PermissionManager;
import com.taobao.zeus.store.UserManager;
import com.taobao.zeus.store.mysql.MysqlFollowManagerOld;
import com.taobao.zeus.store.mysql.persistence.JobPersistenceOld;
import com.taobao.zeus.store.mysql.persistence.Worker;
import com.taobao.zeus.store.mysql.persistence.ZeusUser;
import com.taobao.zeus.util.Tuple;
/**
 * 权限验证，需要的操作权限验证不通过，将抛出异常
 * @author zhoufang
 *
 */
public class PermissionGroupManagerOld implements GroupManagerOld{
	
	
	private Logger log=LogManager.getLogger(PermissionGroupManagerOld.class);
	private GroupManagerOld groupManager;
	public void setGroupManager(GroupManagerOld groupManager) {
		this.groupManager = groupManager;
	}
	@Autowired
	@Qualifier("permissionManager")
	private PermissionManager permissionManager;
	@Autowired
	@Qualifier("userManager")
	private UserManager userManager;
	
	private Boolean isGroupOwner(String uid,GroupBeanOld gb){
		List<String> owners=new ArrayList<String>();
		while(gb!=null){
			if(!owners.contains(gb.getGroupDescriptor().getOwner())){
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb=gb.getParentGroupBean();
		}
		if(owners.contains(uid)){
			return true;
		}
		return false;
	}
	private Boolean isGroupOwner(String uid,String groupId){
		return isGroupOwner(uid, groupManager.getUpstreamGroupBean(groupId));
	}
	
	//自定义修改
	private Boolean isJobOwner(String uid,String jobId){
		JobBeanOld jb=groupManager.getUpstreamJobBean(jobId);
		if(jb.getJobDescriptor().getOwner().equalsIgnoreCase(uid)){
			return true;
		}
		return false;
		//return isGroupOwner(uid, jb.getGroupBean());
	}
	
	public Boolean hasGroupPermission(String uid,String groupId){
		if(isGroupOwner(uid, groupId)){
			return true;
		}
		return permissionManager.hasGroupPermission(uid, groupId);
	}
	public Boolean hasJobPermission(String uid,String jobId){
		if(isJobOwner(uid, jobId)){
			return true;
		}
		return permissionManager.hasJobPermission(uid, jobId);
	}
	//自定义修改
	@Override
	public GroupDescriptor createGroup(String user, String groupName,
			String parentGroup, boolean isDirectory) throws ZeusException {
		//if(hasGroupPermission(user, parentGroup)){
			return groupManager.createGroup(user, groupName, parentGroup, isDirectory);
		//}else{
		//	throw new ZeusException("您无权操作");
		//}
	}

	//自定义修改
	@Override
	public JobDescriptorOld createJob(String user, String jobName,
			String parentGroup, JobRunTypeOld jobType) throws ZeusException {
		//if(hasGroupPermission(user, parentGroup)){
			return groupManager.createJob(user, jobName, parentGroup, jobType);	
		//}else{
		//	throw new ZeusException("您无权操作");
		//}
	}

	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		if(hasGroupPermission(user, groupId)){
			GroupDescriptor gd=groupManager.getGroupDescriptor(groupId);
			if(gd!=null && gd.getOwner().equals(user)){
				groupManager.deleteGroup(user, groupId);
			}
		}else{
			throw new ZeusException("您无权操作");
		}
		
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		if(hasJobPermission(user, jobId)){
			Tuple<JobDescriptorOld, JobStatus> job=groupManager.getJobDescriptor(jobId);
			if(job!=null){
				groupManager.deleteJob(user, jobId);
			}
		}else{
			throw new ZeusException("没有删除的权限");
		}
	}

	@Override
	public GroupBeanOld getDownstreamGroupBean(String groupId) {
		return groupManager.getDownstreamGroupBean(groupId);
	}

	@Override
	public GroupBeanOld getGlobeGroupBean() {
		return groupManager.getGlobeGroupBean();
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		return groupManager.getGroupDescriptor(groupId);
	}

	@Override
	public Tuple<JobDescriptorOld,JobStatus> getJobDescriptor(String jobId) {
		return groupManager.getJobDescriptor(jobId);
	}

	@Override
	public String getRootGroupId() {
		return groupManager.getRootGroupId();
	}

	@Override
	public GroupBeanOld getUpstreamGroupBean(String groupId) {
		return groupManager.getUpstreamGroupBean(groupId);
	}

	@Override
	public JobBeanOld getUpstreamJobBean(String jobId) {
		return groupManager.getUpstreamJobBean(jobId);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group)
			throws ZeusException {
		if(hasGroupPermission(user, group.getId())){
			GroupDescriptor gd=groupManager.getGroupDescriptor(group.getId());
			if(gd!=null){
				groupManager.updateGroup(user, group);
			}
		}else{
			throw new ZeusException("没有更新的权限");
		}
		
	}

	@Override
	public void updateJob(String user, JobDescriptorOld job) throws ZeusException {
		if(hasJobPermission(user, job.getId())){
			Tuple<JobDescriptorOld, JobStatus> old=groupManager.getJobDescriptor(job.getId());
			if(old!=null ){
				List<JobProcesser> hasadd=new ArrayList<JobProcesser>();
				for(Processer p:old.getX().getPreProcessers()){
					if(p instanceof JobProcesser){
						hasadd.add((JobProcesser)p);
					}
				}
				for(Processer p:old.getX().getPostProcessers()){
					if(p instanceof JobProcesser){
						hasadd.add((JobProcesser)p);
					}
				}
				List<JobProcesser> thistime=new ArrayList<JobProcesser>();
				for(Processer p:job.getPreProcessers()){
					if(p instanceof JobProcesser){
						thistime.add((JobProcesser)p);
					}
				}
				for(Processer p:job.getPostProcessers()){
					if(p instanceof JobProcesser){
						thistime.add((JobProcesser)p);
					}
				}
				for(JobProcesser jp:thistime){
					if(jp.getJobId().equals(job.getId())){
						throw new ZeusException("不得将自身设置为自身的处理器");
					}
					boolean exist=false;
					for(JobProcesser jp2:hasadd){
						if(jp2.getId().equalsIgnoreCase(jp.getId())){
							exist=true;
							break;
						}
					}
					if(!exist && !hasJobPermission(user, jp.getJobId())){
						throw new ZeusException("您没有权限将Job："+jp.getJobId() +" 添加到处理单元中");
					}
				}
				groupManager.updateJob(user, job);
			}
		}else{
			throw new ZeusException("没有更新的权限");
		}
		
	}

	@Override
	public Map<String, Tuple<JobDescriptorOld,JobStatus>> getJobDescriptor(Collection<String> jobIds) {
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
	public void grantGroupOwner(String granter, String uid, String groupId) throws ZeusException{
		ZeusUser nextUser = userManager.findByUidFilter(uid);
		if (nextUser.getUserType() != 0) {
			throw new ZeusException("请转给组管理员！");
		}
		if (nextUser.getIsEffective() != 1) {
			throw new ZeusException("请转给有效用户");
		}
		GroupBeanOld gb=groupManager.getUpstreamGroupBean(groupId);
		List<String> owners=new ArrayList<String>();
		while(gb!=null){
			if(!owners.contains(gb.getGroupDescriptor().getOwner())){
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb=gb.getParentGroupBean();
		}
		if(owners.contains(granter)){
			groupManager.grantGroupOwner(granter, uid, groupId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	@Override
	public void grantJobOwner(String granter, String uid, String jobId) throws ZeusException{
		ZeusUser nextUser = userManager.findByUidFilter(uid);
		if (nextUser.getUserType() != 0) {
			throw new ZeusException("请转给组管理员！");
		}
		if (nextUser.getIsEffective() != 1) {
			throw new ZeusException("请转给有效用户");
		}
		JobBeanOld jb=groupManager.getUpstreamJobBean(jobId);
		List<String> owners=new ArrayList<String>();
		owners.add(jb.getJobDescriptor().getOwner());
		GroupBeanOld gb=jb.getGroupBean();
		while(gb!=null){
			if(!owners.contains(gb.getGroupDescriptor().getOwner())){
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb=gb.getParentGroupBean();
		}
		if(owners.contains(granter)){
			groupManager.grantJobOwner(granter, uid, jobId);
		}else{
			throw new ZeusException("您无权操作");
		}
		
	}
	public void addGroupAdmin(String granter,String user, String groupId) throws ZeusException {
		if(isGroupOwner(granter, groupId)){
			permissionManager.addGroupAdmin(user, groupId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public void addJobAdmin(String granter,String user, String jobId) throws ZeusException {
		if(isJobOwner(granter, jobId)){
			permissionManager.addJobAdmin(user, jobId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public void removeGroupAdmin(String granter,String user, String groupId) throws ZeusException {
		if(isGroupOwner(granter, groupId)){
			permissionManager.removeGroupAdmin(user, groupId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public void removeJobAdmin(String granter,String user, String jobId) throws ZeusException {
		if(isJobOwner(granter, jobId)){
			permissionManager.removeJobAdmin(user, jobId);
		}else{
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
	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		return groupManager.getChildrenGroup(groupId);
	}
	@Override
	public List<Tuple<JobDescriptorOld, JobStatus>> getChildrenJob(String groupId) {
		return groupManager.getChildrenJob(groupId);
	}
	@Override
	public GroupBeanOld getDownstreamGroupBean(GroupBeanOld parent) {
		return groupManager.getDownstreamGroupBean(parent);
	}
	@Override
	public void moveJob(String uid, String jobId, String groupId)
			throws ZeusException {
		if(!permissionManager.hasGroupPermission(uid, groupId) || !permissionManager.hasJobPermission(uid, jobId)){
			throw new ZeusException("您无权操作");
		}
		groupManager.moveJob(uid, jobId, groupId);
	}
	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId)
			throws ZeusException {
		if(!permissionManager.hasGroupPermission(uid, groupId) ||
				!permissionManager.hasGroupPermission(uid, newParentGroupId)){
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
	public List<JobPersistenceOld> getAllJobs() {
		return null;
	}
	@Override
	public List<String> getAllDependencied(String jobID) {
		return groupManager.getAllDependencied(jobID);
	}
	@Override
	public List<String> getAllDependencies(String jobID) {
		return groupManager.getAllDependencies(jobID);
	}
	@Override
	public void updateActionList(JobDescriptorOld job) {
		groupManager.updateActionList(job);
	}
	
	/**
	 * 自定义新增  多版本页面
	 * @param gmtModified
	 * @param id
	 * @return
	 * @throws ZeusException 
	 */
	@Override
	public Tuple<JobDescriptorOld, JobStatus> updateVersion(String user ,String gmtModified, String id) throws ZeusException {
		// TODO Auto-generated method stub
		if(hasJobPermission(user, id)){
			return groupManager.updateVersion(user,gmtModified, id);
		}else{
			System.out.println("没有修改版本的权限");
			throw new ZeusException("没有修改版本的权限");
		}
		 
	}

	/**
	 * 自定义新增   指定任务及依赖任务执行   查询要执行的job
	 */
	@Override
	public List<JobPersistenceOld> getJobDescriptors2(final Collection<String> jobIds){
		return groupManager.getJobDescriptors2(jobIds);
	}
	
	

	
}
