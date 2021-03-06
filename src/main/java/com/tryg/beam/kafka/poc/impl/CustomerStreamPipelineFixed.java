package com.tryg.beam.kafka.poc.impl;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.tryg.beam.kafka.poc.function.ParseClaim;
import com.tryg.beam.kafka.poc.function.ParseCustomer;
import com.tryg.beam.kafka.poc.function.ParsePolicy;
import com.tryg.beam.kafka.poc.model.Claim2;
import com.tryg.beam.kafka.poc.model.Customer3;
import com.tryg.beam.kafka.poc.model.Policy2;
import com.tryg.beam.kafka.poc.utils.Join;
import com.tryg.beam.kafka.poc.utils.Output;
import com.tryg.beam.kafka.poc.utils.StreamOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
//import java.time.Duration;

public class CustomerStreamPipelineFixed {

    static Logger logger = Logger.getLogger(CustomerStreamPipelineFixed.class);

    public static class ExtractAndMapCustomerKey
            extends PTransform<PCollection<Customer3>, PCollection<KV<Integer, String>>> {



        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<Customer3> compKeyAndCustInfo) {


            return compKeyAndCustInfo




                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                            .via((Customer3 info) -> KV.of(Integer.parseInt(info.getCustomer().replaceFirst("cust","")), info.getAddress()+","+info.getCustomer()+","+info.getCustomerTime() )));

            // .apply(Max.perKey());

        }
    }

    public static class ExtractAndMapCustomerKey2
            extends PTransform<PCollection<Customer3>, PCollection<KV<Integer, String>>> {



        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<Customer3> compKeyAndCustInfo) {


            return compKeyAndCustInfo
                  //  .apply(Window.<Customer3> into(FixedWindows.of(Duration.standardSeconds(10)))
                    //.triggering(AfterWatermark.pastEndOfWindow()).accumulatingFiredPanes().withAllowedLateness(Duration.standardSeconds(1)))

                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                            .via((Customer3 info) -> KV.of(Integer.parseInt(info.getCustomer().replaceFirst("cust","")), info.getAddress()+","+info.getCustomer()+","+info.getCustomerTime() )));

            // .apply(Max.perKey());

        }
    }
    public static class ExtractAndMapPolicyKey2
            extends PTransform<PCollection<Policy2>, PCollection<KV<Integer, String>>>{



        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<Policy2> compKeyAndPolicyInfo) {


            return compKeyAndPolicyInfo
                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                            .via((Policy2 info) -> KV.of(info.getPolicy(),info.getPvar1()+","+info.getPolicyendtime()+","+info.getPolicy()+","+info.getPolicystarttime()+","+info.getPvar0() )));


        }
    }

    public static class ExtractAndMapPolicyKey
            extends PTransform<PCollection<Policy2>, PCollection<KV<Integer, String>>>{



        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<Policy2> compKeyAndCustInfo) {


            return compKeyAndCustInfo

                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                            .via((Policy2 info) -> KV.of(info.getPolicy(),info.getPvar1()+","+info.getPolicyendtime()+","+info.getPolicy()+","+info.getPolicystarttime()+","+info.getPvar0() )));



        }
    }
    public static class ExtractAndMapClaimKey
            extends PTransform<PCollection<Claim2>, PCollection<KV<Integer, String>>>{



        @Override
        public PCollection<KV<Integer, String>> expand(PCollection<Claim2> compKeyAndClaimInfo) {


            return compKeyAndClaimInfo

                    .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                            .via((Claim2 info) -> KV.of(Integer.parseInt(info.getClaimnumber().split("_")[0]),info.getClaimtime() +","+info.getClaimcounter()+","+info.getClaimnumber()+","+info.getClaimreporttime())));

            // .apply(Max.perKey());

        }
    }

    static class OutputStringValue extends SimpleFunction<KV<Integer,KV<String,String>>, String> {

        private static final long serialVersionUID = 1L;

        public String apply(KV<Integer,KV<String,String>> input) {
            System.out.println("***************Inside FomratStringAsValues********************");
            logger.info("****************Logger Inside formatStringAsValues********************");
            logger.info("LoggerFormatStringValue getKey"+input.getKey());
            System.out.println("FormatStringValue getKey"+input.getKey());
            System.out.println("Format String Values getValue-getKey"+input.getValue().getKey());
            System.out.println("Format String Values getValue-getValue"+input.getValue().getValue());

            String output = input.getValue().getValue();

            return output;

        }
    }


    public static void main(String[] args) throws IOException {

        StreamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StreamOptions.class);
        options.setOutput("/home/statarm/output/output");

        //String outputFile = options.getOutput();
        String bootstrapServers = options.getBootstrapServers();
        long latenessDuration = options.getLatenessDuration();
        long delayDuration = options.getDelayDuration();


        String customerTopic = "customertest2";
        String policyTopic = "policy";
        String claimTopic = "claim";

        Map<String, Object> consumerProps = new HashMap<String, Object>();
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Pipeline pipeline = Pipeline.create(options);

        Trigger trigger1 =
                AfterProcessingTime
                        .pastFirstElementInPane()
                        .plusDelayOf(Duration.standardSeconds(10));
        /**PARSE CUSTOMER*/

        PCollection<Customer3> customerInput = pipeline

                .apply((KafkaIO.<String, String> read().withTopics(ImmutableList.of(customerTopic))
                        .updateConsumerProperties(consumerProps).withBootstrapServers(bootstrapServers)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class))
                        .withoutMetadata())
                .apply(Values.<String> create())
                //.apply(ParseJsons.of(Customer.class));
                .apply("ParseJsonEventFn2", ParDo.of(new ParseCustomer()))
               .apply(Window.<Customer3> into(FixedWindows.of(Duration.standardSeconds(60)))
                       .triggering(AfterWatermark.pastEndOfWindow()
                       .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                               .alignedTo(Duration.standardSeconds(10)))
                       .withLateFirings(AfterProcessingTime.pastFirstElementInPane().alignedTo(Duration.standardSeconds(20))))
                       .withAllowedLateness(Duration.standardMinutes(5))
                       .accumulatingFiredPanes());

        /**PARSE POLICY*/

        PCollection<Policy2> policyInput = pipeline
                .apply((KafkaIO.<String, String> read().withTopics(ImmutableList.of(policyTopic))
                        .updateConsumerProperties(consumerProps).withBootstrapServers(bootstrapServers)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class))
                        .withoutMetadata())
                .apply(Values.<String> create())
                .apply("ParsePolicy", ParDo.of(new ParsePolicy()))
           .apply(Window.<Policy2> into(FixedWindows.of(Duration.standardSeconds(60)))
                   .triggering(AfterWatermark.pastEndOfWindow()
                           .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                   //early update frequency
                                   .alignedTo(Duration.standardSeconds(10)))
                           .withLateFirings(AfterProcessingTime.pastFirstElementInPane().alignedTo(Duration.standardSeconds(20))))
                   .withAllowedLateness(Duration.standardMinutes(5))
                   .accumulatingFiredPanes());
       /**PARSE CLAIM**/
        PCollection<Claim2> claimInput = pipeline

                .apply((KafkaIO.<String, String> read().withTopics(ImmutableList.of(claimTopic))
                        .updateConsumerProperties(consumerProps).withBootstrapServers(bootstrapServers)
                        .withKeyDeserializer(StringDeserializer.class)
                        .withValueDeserializer(StringDeserializer.class))
                        .withoutMetadata())
                .apply(Values.<String> create())
                .apply("ParseJsonEventFn2", ParDo.of(new ParseClaim()))
                .apply(Window.<Claim2> into(FixedWindows.of(Duration.standardSeconds(60)))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                                        //early update frequency
                                        .alignedTo(Duration.standardSeconds(10)))
                                .withLateFirings(AfterProcessingTime.pastFirstElementInPane().alignedTo(Duration.standardSeconds(20))))
                        .withAllowedLateness(Duration.standardMinutes(5))
                        .accumulatingFiredPanes());

        /**CUSTOMER  ********/
        PCollection<KV<Integer,String>> all_customers = customerInput
                .apply(new ExtractAndMapCustomerKey());


        /***POLICY********/
        PCollection<KV<Integer,String>> all_policies = policyInput
                .apply(new ExtractAndMapPolicyKey());

        /***CLAIM*******/

       PCollection<KV<Integer,String>> all_claims = claimInput
                .apply(new ExtractAndMapClaimKey())
                ;


        /**JOIN************** this works now*/
        PCollection<KV<Integer,KV<String,String>>> joinedCustomersAndPolicies= Join.innerJoin3Way(all_customers,all_policies,all_claims);
        //PCollection<KV<Integer,KV<String,String>>> joinedCustomersAndPolicies= Join.innerJoin(all_customers,all_policies);
     //  PCollection<KV<Integer,KV<KV<String,String>,String>>> joinedCustomersPoliciesAndClaims =
              // Join.innerJoin(joinedCustomersAndPolicies,all_claims);

      //  PCollectionList<KV<Integer,String>> collections = PCollectionList.of(all_customers).and(all_policies).and(all_claims);
        //PCollectionList<KV<Integer,String>> collections = PCollectionList.of(all_customers).and(all_policies);
        //PCollection<KV<Integer,String>> merged= collections.apply(Flatten.<KV<Integer,String>>pCollections());


      /**PRINT**/
        logger.info("****************Logger Before joinedCustomersAndPolicies********************");
        //Print customers
        PCollection<String> joinedCustomersAndPoliciesStrings = joinedCustomersAndPolicies.apply(MapElements.via(new OutputStringValue()));
        //joinedCustomersAndPoliciesStrings.apply(new WriteOneFilePerWindows("/home/statarm/output/xxxjoin",2));
        // String s = joinedCustomersAndPoliciesStrings.apply(MapElements.via(new AFPipeline2.FormatAsTextFn2()));

        joinedCustomersAndPolicies.apply(new Output.WriteKvIntKvStrStr2(options.getOutputPrefix(),"StreamjoinedCustPol"));


        all_customers.apply(new Output.WriteObjectAndKeys(options.getOutputPrefix(),"kvOfStreamCust "));
        all_policies.apply(new Output.WriteObjectAndKeys(options.getOutputPrefix(),"kvOfStreamPolicy"));
       all_claims.apply(new Output.WriteObjectAndKeys(options.getOutputPrefix(),"kvOfStreamClaims"));
        // merged.apply(new Output.WriteObjectAndKeys(options.getOutputPrefix(),"kvOfMerged"));

    pipeline.run();
    }
}
