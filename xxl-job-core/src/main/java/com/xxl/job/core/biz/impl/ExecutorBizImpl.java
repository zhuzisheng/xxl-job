package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

/**
 * Created by xuxueli on 17/3/1.
 */
public class ExecutorBizImpl implements ExecutorBiz {
    private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

    @Override
    public ReturnT<String> beat() {
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {

        // isRunningOrHasQueue
        boolean isRunningOrHasQueue = false;
        CopyOnWriteArrayList<JobThread> jobThreads = XxlJobExecutor.loadJobThread(idleBeatParam.getJobId());
        for(JobThread jobThread : jobThreads) {
        	if (jobThread != null && jobThread.isRunningOrHasQueue()) {
        	    isRunningOrHasQueue = true;
        	}

        	if (isRunningOrHasQueue) {
        	    return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
        	}
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        // load old：jobHandler + jobThread
        CopyOnWriteArrayList<JobThread> jobThreads = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
        IJobHandler jobHandler = jobThreads!=null && jobThreads.size() > 0?jobThreads.get(0).getHandler():null;
        String removeOldReason = null;

        // valid：jobHandler + jobThread
        GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
        if (GlueTypeEnum.BEAN == glueTypeEnum) {

            // new jobhandler
            IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

            // valid old jobThread
            if (jobThreads!=null && jobThreads.size() > 0 && jobHandler != newJobHandler) {
                // change handler, need kill old thread
                removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";

                jobThreads = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                jobHandler = newJobHandler;
                if (jobHandler == null) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
                }
            }

        } else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {

            // valid old jobThread
            if (jobThreads != null && jobThreads.size() > 0 &&
                    !(jobThreads.get(0).getHandler() instanceof GlueJobHandler
                        && ((GlueJobHandler) jobThreads.get(0).getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // change handler or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThreads = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                try {
                    IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
                    jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
                }
            }
        } else if (glueTypeEnum!=null && glueTypeEnum.isScript()) {

            // valid old jobThread
            if (jobThreads != null && jobThreads.size() > 0 &&
                    !(jobThreads.get(0).getHandler() instanceof ScriptJobHandler
                            && ((ScriptJobHandler) jobThreads.get(0).getHandler()).getGlueUpdatetime()==triggerParam.getGlueUpdatetime() )) {
                // change script or gluesource updated, need kill old thread
                removeOldReason = "change job source or glue type, and terminate the old job thread.";

                jobThreads = null;
                jobHandler = null;
            }

            // valid handler
            if (jobHandler == null) {
                jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
            }
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
        }

        // executor block strategy
        Boolean parallel = false;
        if (jobThreads != null && jobThreads.size() > 0) {
            ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
            if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {
                // discard when running
            	for(JobThread jobThread : jobThreads) {
					if (jobThread.isRunningOrHasQueue()) {
						return new ReturnT<String>(ReturnT.FAIL_CODE, "block strategy effect："+ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
					}
            	}
            } else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {
                // kill running jobThread
					removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();

					jobThreads = null;
            } else if (ExecutorBlockStrategyEnum.CONCURRENT_EXECUTION == blockStrategy) {
            	parallel = true;
            } else {
                // just queue trigger
            }
        }

        // replace thread (new or exists invalid)
        JobThread jbtd = null;
        if (!parallel && (jobThreads == null || jobThreads.size() == 0)) {
            jbtd  = XxlJobExecutor.registJobThread(triggerParam.getJobId(), triggerParam.getLogId(), jobHandler, removeOldReason);
        } else if(parallel) {
        	jbtd = XxlJobExecutor.registJobThread(triggerParam.getJobId(), triggerParam.getLogId(), jobHandler, parallel);
        } else {
        	jbtd = jobThreads.get(jobThreads.size() - 1);
        }

        // push data to queue
        ReturnT<String> pushResult = jbtd.pushTriggerQueue(triggerParam);
        return pushResult;
    }

    @Override
    public ReturnT<String> kill(KillParam killParam) {
        // kill handlerThread, and create new one
        JobThread jobThread = XxlJobExecutor.loadJobThread(killParam.getJobId(), killParam.getLogId());
        if (jobThread != null) {
        	//String key = String.format("%i-%i", killParam.getJobId(), killParam.getLogId());
            XxlJobExecutor.removeJobThread(killParam.getJobId(), killParam.getLogId(), "scheduling center kill job.");
            //XxlJobExecutor.removeJobThread(key, "scheduling center kill job.");
            return ReturnT.SUCCESS;
        }

        return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread already killed.");
    }

    @Override
    public ReturnT<LogResult> log(LogParam logParam) {
        // log filename: logPath/yyyy-MM-dd/9999.log
        String logFileName = null;
        if(logParam.getLogPath() != null && logParam.getLogPath() != "" && logParam.getLogPath().startsWith("/home")) {
            logFileName = logParam.getLogPath();
        } else {
            logFileName = XxlJobFileAppender.makeLogFileName(new Date(logParam.getLogDateTim()), logParam.getLogId());
        }

        LogResult logResult = XxlJobFileAppender.readLog(logFileName, logParam.getFromLineNum());
        return new ReturnT<LogResult>(logResult);
    }

    @Override
    public ReturnT<String> killbatch(KillParam killParam) {
        List<File> killscripts = new ArrayList<>();
        File batchdir = new File(killParam.getBatchDir());
        if(batchdir.exists() && batchdir.isDirectory()) {
            // try to find kill batch script
            File[] data = batchdir.listFiles(new FilenameFilter() {
                                                @Override
                                                public boolean accept(File dir, String name) {
                                                    return name.equals("kill");
                                                }
                                            });
            if(data.length == 0) {
                File[] dirs = batchdir.listFiles(new FilenameFilter() {
                                        @Override
                                        public boolean accept(File current, String name) {
                                          return new File(current, name).isDirectory();
                                        }
                                    });
                for(File d : dirs) {
                    data = batchdir.listFiles(new FilenameFilter() {
                                                @Override
                                                public boolean accept(File dir, String name) {
                                                    return name.equals("kill");
                                                }
                                            });
                    if(data.length != 0) {
                        for(int i=0; i<data.length; i++) {
                            killscripts.add(data[i]);
                        }
                    }
                }
            } else {
                if(data.length != 0) {
                    for(int i=0; i<data.length; i++) {
                        killscripts.add(data[i]);
                    }
                }
            }
            if(killscripts.size() == 0) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, "Fail to find kill script in batchdir: " + killParam.getBatchDir());
            }
            for(File f : killscripts) {
                ProcessBuilder builder = new ProcessBuilder();
                String killCommand =  f.getAbsolutePath();
                if(killParam.getSoft()) killCommand += " -soft";
                builder.command(killCommand);
                builder.directory(f.getParentFile());
                Process process = null;
                int exitCode = 0;
                String errorMessage = "";
                try {
                    process = builder.start();
                    exitCode = process.waitFor();
                } catch (Exception e) {
                    e.printStackTrace();
                    errorMessage += e.toString();
                    exitCode = 1;
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "Fail to run kill script in batchdir: " + f.getParentFile() + "; Error: " + errorMessage);
                }
                InputStream stderr = process.getErrorStream();
                BufferedReader errorred = new BufferedReader(new InputStreamReader(stderr));
                String line;
                if(exitCode != 0) {
                    try {
                        while ((line = errorred.readLine()) != null) {
                            errorMessage += line;
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if(exitCode != 0) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, "Fail to run kill script in batchdir: " + f.getParentFile() + "; Error: " + errorMessage);
                } 
            }
            return new ReturnT<String>(ReturnT.SUCCESS_CODE, "run kill script in batchdir: " + killParam.getBatchDir() + " PASS");
        } else {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Fail to find the batch dir: " + killParam.getBatchDir());
        }
    }

}
