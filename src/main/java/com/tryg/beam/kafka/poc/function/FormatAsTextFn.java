package com.tryg.beam.kafka.poc.function;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tryg.beam.kafka.poc.model.Customer;

class FormatAsTextFn implements SerializableFunction<Customer, String> {

	Logger logger = LoggerFactory.getLogger(FormatAsTextFn.class);

	private static final long serialVersionUID = 1L;

	public String apply(Customer input) {
		System.out.println(input.toString());
		logger.info(input.toString());
		return input.toString();

	}
}