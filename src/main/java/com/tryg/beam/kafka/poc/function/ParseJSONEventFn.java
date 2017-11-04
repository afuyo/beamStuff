package com.tryg.beam.kafka.poc.function;

import java.io.IOException;

import org.apache.beam.sdk.transforms.DoFn;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.beam.kafka.poc.model.Customer;

public class ParseJSONEventFn extends DoFn<String, Customer> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) throws JsonParseException, JsonMappingException, IOException {
		String jsonString = c.element().toString();
		if (!jsonString.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			Customer customer = mapper.readValue(jsonString, Customer.class);
			c.output(customer);
			// Add code for json parser
		}
	}

}
