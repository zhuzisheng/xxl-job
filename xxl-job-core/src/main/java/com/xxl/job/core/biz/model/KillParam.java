package com.xxl.job.core.biz.model;

import java.io.Serializable;

/**
 * @author xuxueli 2020-04-11 22:27
 */
public class KillParam implements Serializable {
    private static final long serialVersionUID = 42L;

    public KillParam() {
    }
    public KillParam(int jobId) {
        this.jobId = jobId;
    }
    
    public KillParam(int jobId, long logId) {
        this.jobId = jobId;
        this.logId = logId;
    }

    private int jobId;
    private long logId;


    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }
	public long getLogId() {
		return logId;
	}
	public void setLogId(long logId) {
		this.logId = logId;
	}
}