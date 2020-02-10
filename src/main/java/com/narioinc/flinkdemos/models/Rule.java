package com.narioinc.flinkdemos.models;

public class Rule {
	private int mOccurence;
	private long mDuration;
	private String mDeviceType;
	
	public Rule(String deviceType, int occurence, long durationMillis){
		mOccurence = occurence;
		mDuration = durationMillis;
		mDeviceType = deviceType;
	}
	
	public int getOccurence() {
		return mOccurence;
	}
	public void setOccurence(int occurence) {
		this.mOccurence = occurence;
	}
	public long getDuration() {
		return mDuration;
	}
	public void setDuration(long duration) {
		this.mDuration = duration;
	}

	public String getDeviceType() {
		return mDeviceType;
	}

	public void setDeviceType(String deviceType) {
		this.mDeviceType = deviceType;
	}
}
