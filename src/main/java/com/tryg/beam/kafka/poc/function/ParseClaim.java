package com.tryg.beam.kafka.poc.function;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tryg.beam.kafka.poc.model.Claim2;
import com.tryg.beam.kafka.poc.model.Policy2;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;

public class ParseClaim extends DoFn<String, Claim2> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) throws JsonParseException, JsonMappingException, IOException {
		String jsonString = c.element().toString();
		if (!jsonString.isEmpty()) {
			ObjectMapper mapper = new ObjectMapper();
			Claim2 claim = mapper.readValue(jsonString, Claim2.class);
			c.output(claim);
			// Add code for json parser
		}
	}

}
