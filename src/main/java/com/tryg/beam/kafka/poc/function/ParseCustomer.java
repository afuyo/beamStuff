package com.tryg.beam.kafka.poc.function;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.beam.kafka.poc.model.Customer3;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

public class ParseCustomer extends DoFn<String, Customer3> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) throws JsonParseException, JsonMappingException, IOException {
		String jsonString = c.element().toString();
		if (!jsonString.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			Customer3 customer = mapper.readValue(jsonString, Customer3.class);
			//System.out.println("###CusotmerParser"+customer.getCustomer());
			c.output(customer);
			// Add code for json parser
		}
	}

}
