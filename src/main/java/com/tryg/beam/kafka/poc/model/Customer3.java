package com.tryg.beam.kafka.poc.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class Customer3 {

	private String address;
	private String customer;
	private Double customertime;

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

	public Double getCustomerTime() {
		return customertime;
	}

	public void setCustomertime(Double customertime) {
		this.customertime=customertime;
	}

	 

}
