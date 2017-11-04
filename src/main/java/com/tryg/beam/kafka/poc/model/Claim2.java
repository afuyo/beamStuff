package com.tryg.beam.kafka.poc.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Claim2 {


	private Double claimtime;

	public Double getClaimtime() {
		return claimtime;
	}

	public void setClaimtime(Double claimtime) {
		this.claimtime = claimtime;
	}

	public Integer getClaimcounter() {
		return claimcounter;
	}

	public void setClaimcounter(Integer claimcounter) {
		this.claimcounter = claimcounter;
	}

	public String getClaimnumber() {
		return claimnumber;
	}

	public void setClaimnumber(String claimnumber) {
		this.claimnumber = claimnumber;
	}

	public Double getClaimreporttime() {
		return claimreporttime;
	}

	public void setClaimreporttime(Double claimreporttime) {
		this.claimreporttime = claimreporttime;
	}

	private Integer claimcounter;
	private String claimnumber;
	private Double claimreporttime;

	


}
