package com.narioinc.flinkdemos.models;

import com.google.gson.annotations.SerializedName;

public class Device {
	public String getDeviceID() {
		return deviceID;
	}
	public void setDeviceID(String deviceID) {
		this.deviceID = deviceID;
	}
	public String getDeviceName() {
		return deviceName;
	}
	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	@SerializedName("deviceID")
	public String deviceID;
	@SerializedName("deviceName")
	public String deviceName;
	@SerializedName("domain")
	public String domain;
	@SerializedName("deviceType")
	public String deviceType;
	
	public String toString(){
		return this.getDeviceID() + " :: " + this.getDeviceName();		
	}
}
