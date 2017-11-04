package com.tryg.beam.kafka.poc.function;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.beam.kafka.poc.model.Customer3;
import com.tryg.beam.kafka.poc.model.Policy;
import com.tryg.beam.kafka.poc.model.Policy2;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

public class ParsePolicy extends DoFn<String, Policy2> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) throws JsonParseException, JsonMappingException, IOException {
		String jsonString = c.element().toString();
		if (!jsonString.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			Policy2 policy = mapper.readValue(jsonString, Policy2.class);
			c.output(policy);
			// Add code for json parser
		}
	}

}
