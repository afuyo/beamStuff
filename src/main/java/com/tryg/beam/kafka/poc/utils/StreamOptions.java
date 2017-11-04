package com.tryg.beam.kafka.poc.utils;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;


public interface StreamOptions extends PipelineOptions {
	static final long WINDOW_SIZE = 2l;
	static final int NUM_SHARDS = 5;
	static final long DELAY = 10l;
	static final long LATENESS_DURATION = 10l;


	@Description("Prefix for output files, either local path or cloud storage location.")
	@Default.String("/home/statarm/output/")
	String getOutputPrefix();
	void setOutputPrefix(String value);

	@Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
	Long getMinTimestampMillis();

	@Description("the output file prefix")
	void setOutput(String output);

	String getOutput();

	@Description("Fixed window duration, in minutes")
	@Default.Long(WINDOW_SIZE)
	long getWindowSize();

	void setWindowSize(long windowSize);

	void setMinTimestampMillis(Long value);

	@Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
	Long getMaxTimestampMillis();

	void setMaxTimestampMillis(Long value);

	@Default.Integer(NUM_SHARDS)
	@Description("Fixed number of shards to produce per window, or null for runner-chosen sharding")
	Integer getNumShards();

	void setNumShards(Integer numShards);

	/**
	 * Set this window start duration
	 */
	@Description("Duration for sliding window, when the window should start")
	Long getWindowStartDuration();

	void setWindowStartDuration(Long windowStartDuration);

	@Description(" Duration of lateness allowed in minutes")
	@Default.Long(LATENESS_DURATION)
	Long getLatenessDuration();

	void setLatenessDuration(Long latenessDuration);

	@Description(" Duration of Delay allowed in minutes")
	@Default.Long(DELAY)
	Long getDelayDuration();

	void setDelayDuration(Long delayDuration);

	@Description(" Path to Input file ")
	//@Default.String("/home/statarm/output/output")
	String getInput();

	void setInput(String input);

	@Description(" Kafka topic name ")

	String getKafkaTopic();
	void setKafkaTopic(String kafkaTopic);

	@Description(" Kafka Broker IP and port name ")
	@Default.String("localhost:9092")
	String getBootstrapServers();

	void setBootstrapServers(String bootstrapServers);


}
