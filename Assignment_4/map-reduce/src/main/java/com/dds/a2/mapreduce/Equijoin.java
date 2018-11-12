package com.dds.a2.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Hello world!
 *
 */
public class Equijoin { 
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		System.out.println(files[0]);
		Job job = new Job(c, "equijoin");
		job.setJarByClass(Equijoin.class);
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(files[0]));
		FileOutputFormat.setOutputPath(job, new Path(files[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class CustomMapper extends Mapper<DoubleWritable, Text, DoubleWritable, Text> {

		private Text outputValue = new Text();

		private DoubleWritable outputKey = new DoubleWritable();

		public void map(DoubleWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tuplesItr = new StringTokenizer(value.toString());
			while (tuplesItr.hasMoreTokens()) {
				String tuple = tuplesItr.nextToken();
				String[] comps = tuple.split(", ");
				outputKey.set(Double.parseDouble(comps[1]));
				outputValue.set(tuple);
				context.write(outputKey, outputValue);
			}
		}
	}

	public static class CustomReducer extends Reducer<DoubleWritable, Text, Object, Text> {

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> tuples = new ArrayList<String>();
			List<String> table1Tuples = new ArrayList<String>();
			List<String> table2Tuples = new ArrayList<String>();
			Text outputText = new Text();
			StringBuilder outputStringBuilder = new StringBuilder();
			String table1 = null;
			for (Text value : values) {
				tuples.add(value.toString());
			}
			// No point doing anything on ids which isn't present in both tables. For that
			// atleast 2 tuples for an id should be present.
			if (tuples.size() > 1) {
				// First element from first tuple which is name of first table
				table1 = tuples.get(0).split(", ")[0];
				for (String tuple : tuples) {
					if (table1.equals(tuple.split(", ")[0])) {
						table1Tuples.add(tuple);
					} else {
						table2Tuples.add(tuple);
					}
				}
				// Making sure matching tuples in both tables, not just multiple tuples from same table
				if(!table1Tuples.isEmpty() && !table2Tuples.isEmpty()) {
					//output to result different combination of tuples from t1,t2
					for(String table1Tuple: table1Tuples) {
						for(String table2Tuple: table2Tuples) {
							outputStringBuilder.append(table1Tuple);
							outputStringBuilder.append(", ");
							outputStringBuilder.append(table2Tuple);
							outputStringBuilder.append("\n");
						}
					}
					outputText.set(outputStringBuilder.toString().trim());
					context.write(null, outputText);
				}
			}
		}
	}
}
