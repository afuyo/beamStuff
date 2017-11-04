/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tryg.beam.kafka.poc.impl;

import com.tryg.beam.kafka.poc.function.ParseCustomer;
import com.tryg.beam.kafka.poc.model.Customer3;
import com.tryg.beam.kafka.poc.utils.BatchOptions;
import com.tryg.beam.kafka.poc.utils.Join;
import com.tryg.beam.kafka.poc.utils.Output;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
//import org.apache.beam.sdk.extensions.joinlibrary.Join; Can't find it in maven

public class BatchPipeline {

  /**
   * A transform to extract key/timestamp(double) information from Customer, and select max
   * per group of keys. Returns PTransform<KV<K, T>> mapping each distinct key  to the maximum
   * value.
   */
  public static class ExtractAndMaxPerKey
      extends PTransform<PCollection<Customer3>, PCollection<KV<String, Double>>> {



    @Override
    public PCollection<KV<String, Double>> expand(PCollection<Customer3> customerInfo) {


      return customerInfo

              .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles()))
                      .via((Customer3 info) -> KV.of(info.getCustomer(), info.getCustomerTime())))

      .apply(Max.perKey())
      .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(),TypeDescriptors.doubles()))
              /**
               * Format output to K,V of composite key+timestamp,timestamp
               */
      .via((KV<String,Double> input)-> KV.of(input.getKey()+input.getValue().toString(),input.getValue()) ));


    }
  }

  public static class ExtractCompositeKey
          extends PTransform<PCollection<Customer3>, PCollection<KV<String, String>>>{



    @Override
    public PCollection<KV<String, String>> expand(PCollection<Customer3> compKeyAndCustInfo) {


      return compKeyAndCustInfo

              .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via((Customer3 info) -> KV.of(info.getCustomer()+info.getCustomerTime(), info.getAddress()+","+info.getCustomer()+","+info.getCustomerTime() )));

             // .apply(Max.perKey());

    }
  }




  /**
   * Run a batch pipeline.
   */
  public static void main(String[] args) throws Exception {

    BatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchOptions.class);
    Pipeline pipeline = Pipeline.create(options);

           //Read&Parse Customer
           PCollection<Customer3> cust_input = pipeline
            .apply(TextIO.read().from("/home/statarm/input/customerBatch.json"))
            .apply("ParseCustomer", ParDo.of(new ParseCustomer()));

           //Collection of most current customers key+timestamp
           PCollection<KV<String,Double>> current_cust_keys =cust_input.apply("ExtractAndMaxPerKey", new ExtractAndMaxPerKey());

           //Collection of all customers key+timestam+customer info
          PCollection<KV<String,String>> all_customers = cust_input.apply(new ExtractCompositeKey());
           //Join

           String nullValue = "nullValue";
   // PCollection<KV<String, KV<Double, String>>> joinedCutomers= Join.innerJoin(current_cust_keys,all_customers);
    PCollection<KV<String, KV<Double, String>>> joinedCutomers= Join.leftOuterJoin(current_cust_keys,all_customers,nullValue);







           //Print it for debugging purposes
            joinedCutomers.apply(new Output.WriteJoinedCustomers(options.getOutputPrefix()));

            current_cust_keys.apply(new Output.WriteCurrentCustomerKeys(options.getOutputPrefix()));

    // Run the batch pipeline.
    pipeline.run();
  }
}
