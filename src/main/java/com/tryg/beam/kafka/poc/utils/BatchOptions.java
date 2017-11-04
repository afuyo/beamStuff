package com.tryg.beam.kafka.poc.utils;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;


public interface BatchOptions extends PipelineOptions {

  @Description("Prefix for output files, either local path or cloud storage location.")
  @Default.String("/home/statarm/output/")
  String getOutputPrefix();
  void setOutputPrefix(String value);
}
