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

package com.tryg.beam.kafka.poc.utils;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Int;

import java.util.TimeZone;

/**
 * Helpers for writing output
 */
public class Output {



  private static class Base<InputT> extends PTransform<PCollection<InputT>, PDone> {

    private final String outputPrefix;
    private final String fileName;
    
    protected MapContextElements<InputT, String> objToString;

    public Base(String outputPrefix, String fileName) {
      this.outputPrefix = outputPrefix;
      this.fileName = fileName;
    }

    @Override
    public PDone expand(PCollection<InputT> input) {
      String outputFilename = outputPrefix + fileName;

      PCollection<String> formatted = input.apply(objToString);

      if (input.isBounded().equals(IsBounded.BOUNDED)) {
        formatted.apply(TextIO.write().to(outputFilename));
      } else {
        formatted.apply(ParDo.of(new UnboundedWriteIO(outputFilename)));
      }

      return PDone.in(input.getPipeline());
    }
  }







    public static class WriteCurrentCustomerKeys extends Base<KV<String, Double>> {
        public WriteCurrentCustomerKeys(String outputPrefix) {
            this(outputPrefix, "curr_cust");
        }

        public WriteCurrentCustomerKeys(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<String, Double>, String>via((KV<DoFn<KV<String, Double>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "cust: " + c.getKey().element().getKey() + " current:"
                                + c.getKey().element().getValue();
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }

    public static class WriteJoinedCustomers extends Base<KV<String, KV<Double, String>>>  {
        public WriteJoinedCustomers(String outputPrefix) {
            this(outputPrefix, "joined_cust");
        }

        public WriteJoinedCustomers(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<String, KV<Double, String>>, String>via((KV<DoFn<KV<String, KV<Double, String>>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "cust: " + c.getKey().element().getKey() + " current:"
                                + c.getKey().element().getValue();
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }


    public static class WriteKvIntKvStrStr extends Base<KV<Integer, KV<String, String>>>  {
        public WriteKvIntKvStrStr(String outputPrefix) {
            this(outputPrefix, "someFileName");
        }

        public WriteKvIntKvStrStr(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<Integer, KV<String, String>>, String>via((KV<DoFn<KV<Integer, KV<String, String>>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "key: " + c.getKey().element().getKey() + " elements: "
                                + c.getKey().element().getValue().getKey()
                                +" second: "+ c.getKey().element().getValue().getValue()
                                ;
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }
    public static class WriteKvIntKvStrStr2 extends Base<KV<Integer, KV<String, String>>>  {
        public WriteKvIntKvStrStr2(String outputPrefix) {
            this(outputPrefix, "someFileName");
        }

        public WriteKvIntKvStrStr2(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<Integer, KV<String, String>>, String>via((KV<DoFn<KV<Integer, KV<String, String>>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "key: " + c.getKey().element().getKey() + " elements: "
                                + c.getKey().element().getValue()
                              //  + c.getKey().element().getValue().getKey()
                               // +" second: "+ c.getKey().element().getValue().getValue()
                                ;
                        System.out.println("WriteKvIntKvStrStr2");
                        System.out.println(fileName+output);
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }


    public static class WriteCustomerSums extends Base<KV<String, Integer>> {
        public WriteCustomerSums(String outputPrefix) {
            this(outputPrefix, "cust_sums");
        }

        public WriteCustomerSums(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<String, Integer>, String>via((KV<DoFn<KV<String, Integer>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "cust: " + c.getKey().element().getKey() + " sums"
                                + c.getKey().element().getValue();
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }

    public static class WritePoliciesAndKeys extends Base<KV<Integer, String>> {
        public WritePoliciesAndKeys (String outputPrefix) {
            this(outputPrefix, "all_policies");
        }

        public WritePoliciesAndKeys(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<Integer,String>, String>via((KV<DoFn<KV<Integer,String>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "policy: " + c.getKey().element().getKey() + " element"
                                + c.getKey().element().getValue();
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }

    public static class WriteKvIntStr extends Base<KV<Integer, String>> {
        public WriteKvIntStr (String outputPrefix) {
            this(outputPrefix, "all_policies");
        }

        public WriteKvIntStr(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<Integer,String>, String>via((KV<DoFn<KV<Integer,String>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "key: " + c.getKey().element().getKey() + " element: "
                                + c.getKey().element().getValue();
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }

    public static class WriteKvIntKVKvStrStrStr extends Base<KV<Integer,KV<KV<String,String>,String>>> {
        public WriteKvIntKVKvStrStrStr  (String outputPrefix) {
            this(outputPrefix, "joinedCustPolClaim");
        }

        public WriteKvIntKVKvStrStrStr(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<Integer,KV<KV<String,String>,String>>, String>via((KV<DoFn<KV<Integer,KV<KV<String,String>,String>>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "key: " + c.getKey().element().getKey() + " element: "
                                + c.getKey().element().getValue();
                        c.getKey().output(output);

                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }

    public static class WriteObjectAndKeys extends Base<KV<Integer, String>> {
        public WriteObjectAndKeys (String outputPrefix) {
            this(outputPrefix, "all_policies");
        }

        public WriteObjectAndKeys(String outputPrefix, String fileName) {
            super(outputPrefix, fileName);

            objToString = MapContextElements
                    .<KV<Integer,String>, String>via((KV<DoFn<KV<Integer,String>, String>.ProcessContext, BoundedWindow> c) -> {
                        String output = "policy: " + c.getKey().element().getKey() + " element: "
                                + c.getKey().element().getValue();
                        c.getKey().output(output);
                        System.out.println(fileName+c.getKey().element().getValue());
                        return null;
                    }).withOutputType(TypeDescriptors.strings());
        }
    }



}
