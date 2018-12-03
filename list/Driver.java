package list;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(LabMapper.class);

		job.setMapperClass(LabMapper.class);
		job.setReducerClass(LabReducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MedStdDevWritable.class);

		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, outputDir);

		FileSystem hdfs = FileSystem.get(conf);

		if (hdfs.exists(outputDir)) {
			hdfs.delete(outputDir, true);
		}
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);
	}
}
