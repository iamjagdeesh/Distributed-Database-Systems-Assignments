package com.dds.a2.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//args[0] = classname, args[1] = inputFile, args[2] = outputFile
		Configuration c = new Configuration();
		Job job = new Job(c, "equijoin");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(CustomMapper.class);
		job.setReducerClass(CustomReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class CustomMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		private Text outputValue = new Text();

		private DoubleWritable outputKey = new DoubleWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tuplesItr = new StringTokenizer(value.toString(), "\n");
			while (tuplesItr.hasMoreTokens()) {
				String tuple = tuplesItr.nextToken();
				StringTokenizer compsItr = new StringTokenizer(tuple, ", ");
				String tableName = compsItr.nextToken();
				String id = compsItr.nextToken();
				outputKey.set(Double.parseDouble(id));
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
			if(tuples.size() < 2) {
				// No point doing anything on ids which isn't present in both tables. For that
				// atleast 2 tuples for an id should be present.
				return;
			} else {
				// First element from first tuple which is name of first table
				table1 = tuples.get(0).split(", ")[0];
				for (String tuple : tuples) {
					if (table1.equals(tuple.split(", ")[0])) {
						table1Tuples.add(tuple);
					} else {
						table2Tuples.add(tuple);
					}
				}
				// Making sure matching tuples in both tables, not just multiple tuples from
				// same table
				if(table1Tuples.size() == 0 || table2Tuples.size() == 0) {
					return;
				}else {
					// output to result different combination of tuples from table1,table2
					for (String table1Tuple : table1Tuples) {
						for (String table2Tuple : table2Tuples) {
							outputStringBuilder.append(table1Tuple).append(", ").append(table2Tuple).append("\n");
						}
					}
					outputText.set(outputStringBuilder.toString().trim());
					context.write(null, outputText);
				}
			}
		}
	}
}
