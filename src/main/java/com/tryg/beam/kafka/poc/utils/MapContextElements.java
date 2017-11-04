package com.tryg.beam.kafka.poc.utils;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;


public class MapContextElements<InputT, OutputT> extends PTransform<PCollection<InputT>, PCollection<OutputT>> {


  public static <InputT, OutputT> MissingOutputTypeDescriptor<InputT, OutputT> via(
      SerializableFunction<KV<DoFn<InputT, OutputT>.ProcessContext, BoundedWindow>, OutputT> fn) {
    return new MissingOutputTypeDescriptor<>(fn);
  }


  public static final class MissingOutputTypeDescriptor<InputT, OutputT> {

    private final SerializableFunction<KV<DoFn<InputT, OutputT>.ProcessContext, BoundedWindow>, OutputT> fn;

    private MissingOutputTypeDescriptor(
        SerializableFunction<KV<DoFn<InputT, OutputT>.ProcessContext, BoundedWindow>, OutputT> fn) {
      this.fn = fn;
    }

    public MapContextElements<InputT, OutputT> withOutputType(TypeDescriptor<OutputT> outputType) {
      return new MapContextElements<>(fn, outputType);
    }
  }

  ///////////////////////////////////////////////////////////////////

  private final SerializableFunction<KV<DoFn<InputT, OutputT>.ProcessContext, BoundedWindow>, OutputT> fn;
  private final transient TypeDescriptor<OutputT> outputType;

  private MapContextElements(SerializableFunction<KV<DoFn<InputT, OutputT>.ProcessContext, BoundedWindow>, OutputT> fn,
      TypeDescriptor<OutputT> outputType) {
    this.fn = fn;
    this.outputType = outputType;
  }

  @Override
  public PCollection<OutputT> expand(PCollection<InputT> input) {
    return input.apply("Map", ParDo.of(new DoFn<InputT, OutputT>() {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow w) {
        fn.apply(KV.of(c, w));
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        MapContextElements.this.populateDisplayData(builder);
      }
    })).setTypeDescriptor(outputType);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("mapFn", fn.getClass()).withLabel("Map Function"));
  }
}
