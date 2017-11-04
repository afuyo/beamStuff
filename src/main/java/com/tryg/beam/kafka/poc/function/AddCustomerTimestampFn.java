package com.tryg.beam.kafka.poc.function;

import java.io.IOException;
import java.sql.Timestamp;

import org.apache.beam.sdk.extensions.jackson.ParseJsons;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.beam.kafka.poc.model.Customer;

/**
 * Adding custom timestamp as event time input format : <userId>, <timestamp>,
 * <username> example : 30,2017-08-11 2:01:30, Sam
 * 
 * @author statnit
 *
 */
public class AddCustomerTimestampFn implements SerializableFunction<KafkaRecord<String, String>, Instant> {

	private static final long serialVersionUID = 1L;

	@Override
	public Instant apply(KafkaRecord<String, String> input) {
		System.out.println(" Inside Timestamp fn==========================");
		String value = input.getKV().getValue();
		System.out.println(value);
		if (!value.trim().isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			try {
				Customer customer = mapper.readValue(value, Customer.class);
				String customerTimestamp = customer.getTimestamp();
				Long timestamp = Timestamp.valueOf(customerTimestamp).getTime();
				Instant instant = new Instant(timestamp);
				System.out.println(timestamp);
				return instant;

			} catch (JsonParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return Instant.now();
			} catch (JsonMappingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return Instant.now();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return Instant.now();
			}

		} else {
			return Instant.now();
		}
		 

	}
}