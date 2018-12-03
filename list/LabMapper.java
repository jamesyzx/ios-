package list;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LabMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	Text brand = new Text();
	DoubleWritable rating = new DoubleWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		
		try {
			brand.set(values[0]);
			rating.set(Double.valueOf(values[3]));
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		context.write(brand, rating);
	}

}
