package com.tryg.beam.kafka.poc.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Policy2 {

	private int pvar1;
	private String policyendtime;


	private int policy;
	private String policystarttime;
	private int pvar0;

	

	public int getPolicy() {
		return policy;
	}

	public void setPolicy(int policy) {
		this.policy = policy;
	}


	public String getPolicyendtime() {
		return policyendtime;
	}

	public void setPolicyendtime(String policyendtime) {
		this.policyendtime = policyendtime;
	}

	public String getPolicystarttime() {
		return policystarttime;
	}

	public void setPolicystarttime(String policystarttime) {
		this.policystarttime = policystarttime;
	}


	public int getPvar0() {
		return pvar0;
	}

	public void setPvar0(int pvar0) {
		this.pvar0 = pvar0;
	}

	public int getPvar1() {
		return pvar1;
	}

	public void setPvar1(int pvar1) {
		this.pvar1 = pvar1;
	}
	

}
