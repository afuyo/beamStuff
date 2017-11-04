package com.tryg.beam.kafka.poc.storage;


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static com.google.common.base.Verify.verifyNotNull;

import java.util.Random;

import javax.annotation.Nullable;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * A {@link DoFn} that writes elements to files with names deterministically
 * derived from the lower and upper bounds of their key (an
 * {@link IntervalWindow}).
 *
 * <p>
 * This is test utility code, not for end-users, so examples can be focused on
 * their primary lessons.
 */
public class WriteOneFilePerWindows extends PTransform<PCollection<String>, PDone> {
	 
	private static final long serialVersionUID = 1L;
	private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();
	private String filenamePrefix;
	@Nullable
	private Integer numShards;

	public WriteOneFilePerWindows(String filenamePrefix, Integer numShards) {
		this.filenamePrefix = filenamePrefix;
		this.numShards = numShards;
	}

	@Override
	public PDone expand(PCollection<String> input) {

		System.out.println("------------- Inside WriteFile:expand() method-------------------");
		System.out.println("**********"+input.toString().toString());
		// filenamePrefix may contain a directory and a filename component. Pull
		// out only the filename
		// component from that path for the PerWindowFiles.
		String prefix = "";
		ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
		if (!resource.isDirectory()) {
			prefix = verifyNotNull(resource.getFilename(),
					"A non-directory resource should have a non-null filename: %s", resource);
		}

		System.out.println(" Resource : ");
		System.out.println(resource);
		try {
			TextIO.Write write = TextIO.write().to(resource.getCurrentDirectory())
					.withFilenamePolicy(new PerWindowFiles(prefix)).withWindowedWrites();
			System.out.println(" Value of Write : ");
			System.out.println(write);
			if (numShards != null) {
				write = write.withNumShards(numShards);
			}

			return input.apply(write);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * A {@link FilenamePolicy} produces a base file name for a write based on
	 * metadata about the data being written. This always includes the shard
	 * number and the total number of shards. For windowed writes, it also
	 * includes the window and pane index (a sequence number assigned to each
	 * trigger firing).
	 */
	public static class PerWindowFiles extends FilenamePolicy {
 
		private static final long serialVersionUID = 1L;
		private final String prefix;

		public PerWindowFiles(String prefix) {
			this.prefix = prefix;
		}

		public String filenamePrefixForWindow(IntervalWindow window) {
			return String.format("%s-%s-%s", prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
		}

		@Override
		public ResourceId windowedFilename(ResourceId outputDirectory, WindowedContext context, String extension) {
			IntervalWindow window = (IntervalWindow) context.getWindow();
			System.out.println(" Start of the Window time : ");
			System.out.println(window.start().toString());
			System.out.println(" End of the Window time : ");
			System.out.println(window.end().toString());

			Random rand = new Random();

			int random_number = rand.nextInt(50) + 1;

			String filename = String.format("%s-%s-of-%s%s-%s", filenamePrefixForWindow(window),
					context.getShardNumber(), context.getNumShards(), extension, random_number);
			return outputDirectory.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
		}

		@Override
		public ResourceId unwindowedFilename(ResourceId outputDirectory, Context context, String extension) {
			throw new UnsupportedOperationException("Unsupported.");
		}
	}
}
