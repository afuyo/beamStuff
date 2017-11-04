package com.tryg.beam.kafka.poc.function;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.beam.kafka.poc.model.Customer;
import com.tryg.beam.kafka.poc.model.Customer2;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

public class ParseJSONEventFn2 extends DoFn<String, Customer2> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) throws JsonParseException, JsonMappingException, IOException {
		String jsonString = c.element().toString();
		if (!jsonString.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			Customer2 customer = mapper.readValue(jsonString, Customer2.class);
			String customerId = customer.getCustomer();
			int custId = 0;
			if(!customerId.isEmpty()) {
				custId = Integer.parseInt(customerId.replaceFirst("cust",""));
			}
            customer.setCustomerId(custId);
			c.output(customer);
			// Add code for json parser
		}
	}

}
