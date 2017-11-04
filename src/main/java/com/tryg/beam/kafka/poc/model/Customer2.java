package com.tryg.beam.kafka.poc.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Customer2 {

	private Integer customerId;
	private String address;
	private String customer;
	private Integer customertime;

	public Integer getCustomerId() {
		return customerId;
	}
    public void setCustomerId(Integer customerId) {this.customerId=customerId;}
	public String getCustomer() {
		return customer;
	}

	public void setCustomer(String customer) {
		this.customer = customer;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Integer getCustomerTime() {
		return customertime;
	}

	public void setCustomertime(Integer customertime) {
		this.customertime=customertime;
	}

	 

}
