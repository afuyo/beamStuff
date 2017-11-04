package com.tryg.beam.kafka.poc.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class FormatAsStringFn extends DoFn<KV<String,String>,String> {

    @ProcessElement
    public void processElement(ProcessContext c)  {
             String leftValue = c.element().getKey().toString();
             String  rightValue = c.element().getValue();
             String results = leftValue+rightValue;
             System.out.println("******Results leftValue "+leftValue);
             c.output(results);

        }
    }

