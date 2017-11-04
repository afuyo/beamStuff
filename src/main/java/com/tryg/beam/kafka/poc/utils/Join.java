package com.tryg.beam.kafka.poc.utils;

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.repackaged.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;


public class Join {


    public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> innerJoin(
            final PCollection<KV<K, V1>> leftCollection, final PCollection<KV<K, V2>> rightCollection) {

        System.out.println("***************************** Inside Join inner join method **************************");
        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .apply(CoGroupByKey.<K>create());

        System.out.println(coGbkResultCollection);

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("***************************** Inside Join. coGBkCollection apply method **************************");
                        System.out.println(c.element().getKey());
                        System.out.println(c.element().getValue());
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                        for (V1 leftValue : leftValuesIterable) {
                            for (V2 rightValue : rightValuesIterable) {
                                c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                            }
                         System.out.println("******Inside Join########"+e.getValue());
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }

    public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> leftOuterJoin(
            final PCollection<KV<K, V1>> leftCollection,
            final PCollection<KV<K, V2>> rightCollection,
            final V2 nullValue) {
       // checkNotNull(leftCollection);
        //checkNotNull(rightCollection);
        //checkNotNull(nullValue);

        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .apply(CoGroupByKey.<K>create());

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);
                        System.out.println("#############LeftOUterJOIN "+c.element().getValue());
                        for (V1 leftValue : leftValuesIterable) {
                            if (rightValuesIterable.iterator().hasNext()) {
                                for (V2 rightValue : rightValuesIterable) {
                                    c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                                }
                            } else {
                                c.output(KV.of(e.getKey(), KV.of(leftValue, nullValue)));
                            }
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }



    public static <K, V1, V2> PCollection<KV<K, KV<V1, V2>>> rightOuterJoin(
            final PCollection<KV<K, V1>> leftCollection,
            final PCollection<KV<K, V2>> rightCollection,
            final V1 nullValue) {
       // checkNotNull(leftCollection);
       // checkNotNull(rightCollection);
       // checkNotNull(nullValue);

        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .apply(CoGroupByKey.<K>create());

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);

                        for (V2 rightValue : rightValuesIterable) {
                            if (leftValuesIterable.iterator().hasNext()) {
                                for (V1 leftValue : leftValuesIterable) {
                                    c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                                }
                            } else {
                                c.output(KV.of(e.getKey(), KV.of(nullValue, rightValue)));
                            }
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }


    /**
     *
     *
     *
     *
     */


    public static <K, V1, V2, V3> PCollection<KV<K, KV<V1, V2>>> innerJoin3Way(
            final PCollection<KV<K, V1>> leftCollection,
            final PCollection<KV<K, V2>> rightCollection
            ,final PCollection<KV<K, V3>> thirdCollection)
 {
//KV<Integer,KV<KV<String,String>,String>>
// KV<K,KV<KV<V1,V2>,V3>>
        System.out.println("***************************** Inside Join inner join method **************************");
        final TupleTag<V1> v1Tuple = new TupleTag<>();
        final TupleTag<V2> v2Tuple = new TupleTag<>();
        final TupleTag<V3> v3Tuple = new TupleTag<>();

        PCollection<KV<K, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(v1Tuple, leftCollection)
                        .and(v2Tuple, rightCollection)
                        .and(v3Tuple,thirdCollection)
                        .apply(CoGroupByKey.<K>create());

        System.out.println(coGbkResultCollection);

        return coGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<K, CoGbkResult>, KV<K, KV<V1, V2>>>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println("***************************** Inside Join 333. coGBkCollection apply method **************************");
                        System.out.println("****************getKey3"+c.element().getKey());
                        System.out.println(c.element().getKey());
                        System.out.println(c.element().getValue());
                        KV<K, CoGbkResult> e = c.element();

                        Iterable<V1> leftValuesIterable = e.getValue().getAll(v1Tuple);
                        Iterable<V2> rightValuesIterable = e.getValue().getAll(v2Tuple);
                        Iterable<V3> thirdValuesIterable = e.getValue().getAll(v3Tuple);

                        for(V3 thirdValue : thirdValuesIterable)
                        {
                            System.out.println("****thirdvalue"+thirdValue.toString());
                        }

                        for (V1 leftValue : leftValuesIterable) {
                            for (V2 rightValue : rightValuesIterable) {
                                c.output(KV.of(e.getKey(), KV.of(leftValue, rightValue)));
                            }
                            System.out.println("******leftvalue########"+leftValue.toString());
                        }
                    }
                }))
                .setCoder(KvCoder.of(((KvCoder) leftCollection.getCoder()).getKeyCoder(),
                        KvCoder.of(((KvCoder) leftCollection.getCoder()).getValueCoder(),
                                ((KvCoder) rightCollection.getCoder()).getValueCoder())));
    }



}
