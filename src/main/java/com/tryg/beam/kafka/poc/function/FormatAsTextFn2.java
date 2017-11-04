package com.tryg.beam.kafka.poc.function;

import com.tryg.beam.kafka.poc.model.Customer;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FormatAsTextFn2 implements SerializableFunction<String, String> {

	Logger logger = LoggerFactory.getLogger(FormatAsTextFn2.class);

	private static final long serialVersionUID = 1L;

	public String apply(String input) {
		System.out.println("********Serializable"+input.toString());
		logger.info(input.toString());
		return input.toString();

	}
}