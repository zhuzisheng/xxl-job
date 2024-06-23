package com.xxl.job.admin.core.complete;

import com.xxl.job.admin.SpringContext;
import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.thread.JobTriggerPoolHelper;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.context.XxlJobContext;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * @author xuxueli 2020-10-30 20:43:10
 */
public class XxlJobCompleter {
    private static Logger logger = LoggerFactory.getLogger(XxlJobCompleter.class);
    //private static ConcurrentHashMap<String, Integer> jobLogIdMap = new ConcurrentHashMap();
    private static RedissonClient redissonClient = SpringContext.getBean(RedissonClient.class);
    /**
     * common fresh handle entrance (limit only once)
     *
     * @param xxlJobLog
     * @return
     */
    public static int updateHandleInfoAndFinish(XxlJobLog xxlJobLog) {

        // update handle status into db
        XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateHandleInfo(xxlJobLog);
        // finish
        finishJob(xxlJobLog);

        // text最大64kb 避免长度过长
        if (xxlJobLog.getHandleMsg().length() > 15000) {
            xxlJobLog.setHandleMsg( xxlJobLog.getHandleMsg().substring(0, 15000) );
        }

        // fresh handle
        return XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateHandleInfo(xxlJobLog);
    }


    /**
     * do somethind to finish job
     */
    private static void finishJob(XxlJobLog xxlJobLog){

        // 1、handle success, to trigger child job
        String triggerChildMsg = null;
        if (XxlJobContext.HANDLE_CODE_SUCCESS == xxlJobLog.getHandleCode()) {
            XxlJobInfo xxlJobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(xxlJobLog.getJobId());
            String globalRunId = xxlJobLog.getGlobalRunId();
            String executorParam = null;
            if(globalRunId != null) {
                executorParam = "GLOBAL_RUNID=" + xxlJobLog.getGlobalRunId();
                //redissonClient.
                String redisLock = globalRunId + "_" + String.valueOf(xxlJobLog.getJobId());
                if(redissonClient.getMap("childJobId") != null && redissonClient.getMap("childJobId").containsKey(redisLock)) {
                    logger.info("removing redis key " + redisLock);
                    redissonClient.getMap("childJobId").remove(redisLock);
                }
                //redissonClient.getLock(redisLock).unlock(); 
                //if(jobLogIdMap.containsKey(pkey)) jobLogIdMap.remove(pkey);
            }
            if (xxlJobInfo!=null && xxlJobInfo.getChildJobId()!=null && xxlJobInfo.getChildJobId().trim().length()>0) {
                triggerChildMsg = "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>"+ I18nUtil.getString("jobconf_trigger_child_run") +"<<<<<<<<<<< </span><br>";

                String[] childJobIds = xxlJobInfo.getChildJobId().split(",");
                for (int i = 0; i < childJobIds.length; i++) {
                    int childJobId = (childJobIds[i]!=null && childJobIds[i].trim().length()>0 && isNumeric(childJobIds[i]))?Integer.valueOf(childJobIds[i]):-1;
                    if (childJobId > 0) {
                        XxlJobInfo childJobInfo =  XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(childJobId);
                        String parentJobIdString = childJobInfo.getParentJobId();
                        if(parentJobIdString != null) {
                            String[] idstr = parentJobIdString.split(",");
                            List<Long> parentids = new ArrayList<Long>();
                            for(int j=0; j<idstr.length; j++) {
                                Integer pid = Integer.valueOf(idstr[j]);
                                if(pid.intValue() != xxlJobInfo.getId()) {
                                    parentids.add((long)pid.intValue());
                                }
                            }
                            logger.info("parent id " + String.join(",", idstr));
                            if(parentids.size() > 0) {
                                List<XxlJobLog> logs = XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().getByJobIdsAndGlobalRunId(parentids, globalRunId);
                                List<XxlJobLog> passed = logs.stream().filter(x-> x.getHandleCode() == XxlJobContext.HANDLE_CODE_SUCCESS).collect(Collectors.toList());
                                //synchronized(jobLogIdMap) {
                                String jobLogIdKey = globalRunId + "_" + String.valueOf(childJobId);
                                RLock lock = redissonClient.getLock(jobLogIdKey);
                                try {
                                    try {
                                        lock.tryLock(10, 10, TimeUnit.SECONDS);
                                    } catch(InterruptedException e) {
                                        logger.warn("Fail to lock " + jobLogIdKey + ".");
                                        continue;
                                    }
                                    Boolean kexists = redissonClient.getMap("childJobId").containsKey(jobLogIdKey);
                                    if(passed.size() != logs.size() || logs.size() == 0 || kexists) {
                                        logger.info("parent log ids " + logs.stream().map(x -> Long.valueOf(x.getId()).toString()).collect(Collectors.toList()).toString());
                                        logger.info("passed log ids " + passed.stream().map(x -> Long.valueOf(x.getId()).toString()).collect(Collectors.toList()).toString());
                                        logger.info("redis map childJobId contains key " + jobLogIdKey + " exists " + kexists.toString());
                                        logger.info("Trigger conditions unsatisfied. Skipping...");
                                        continue; 
                                    }
                                    redissonClient.getMap("childJobId").put(jobLogIdKey, 1);
                                    //jobLogIdMap.put(jobLogIdKey, 1);
                                } finally {
                                    lock.unlock();
                                }
                            }

                        } 

                        JobTriggerPoolHelper.trigger(childJobId, TriggerTypeEnum.PARENT, -1, null, executorParam, null);
                        ReturnT<String> triggerChildResult = ReturnT.SUCCESS;

                        // add msg
                        triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg1"),
                                (i+1),
                                childJobIds.length,
                                childJobIds[i],
                                (triggerChildResult.getCode()==ReturnT.SUCCESS_CODE?I18nUtil.getString("system_success"):I18nUtil.getString("system_fail")),
                                triggerChildResult.getMsg());
                    } else {
                        triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg2"),
                                (i+1),
                                childJobIds.length,
                                childJobIds[i]);
                    }
                }

            }
        }

        if (triggerChildMsg != null) {
            xxlJobLog.setHandleMsg( xxlJobLog.getHandleMsg() + triggerChildMsg );
        }

        // 2、fix_delay trigger next
        // on the way
        //logger.info("size of jobLogIds " + String.valueOf(jobLogIds.size()));
    }

    private static boolean isNumeric(String str){
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

}
