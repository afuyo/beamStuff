package com.tryg.beam.kafka.poc.function;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

/**
 * Adding custom timestamp as event time input format : <userId>, <timestamp>,
 * <username> example : 30,2017-08-11 2:01:30, Sam
 * 
 * @author statnit
 *
 */
public class AddPolicyTimestampFn implements SerializableFunction<KafkaRecord<String, String>, Instant> {

	private static final long serialVersionUID = 1L;

	@Override
	public Instant apply(KafkaRecord<String, String> input) {

		String value = input.getKV().getValue();
		if (!value.trim().isEmpty()) {
			return Instant.now();
		//	ObjectMapper mapper = new ObjectMapper();
			 
			//	Policy policy = mapper.readValue(value, Policy.class);
			//	String policyStartTime = policy.getPolicyStartTime();
			//	Long timestamp = Timestamp.valueOf(policyStartTime).getTime();
			//	Instant instant = new Instant(timestamp);
			//	System.out.println(timestamp);
			//	return instant;

			 
		} else {
			return Instant.now();
		}
		 

	}
}