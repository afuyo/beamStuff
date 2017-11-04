package com.tryg.beam.kafka.poc.function;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class FormatClaimKey extends DoFn<KV<String,String>,KV<Integer,String>> {

    @ProcessElement
    public void processElement(ProcessContext c)  {
             Integer claimKey = Integer.parseInt(c.element().getKey().split("_")[0]);
             String  claimValue = c.element().getValue();

             c.output(KV.of(claimKey,claimValue));

        }
    }

