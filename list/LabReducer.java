package list;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LabReducer extends Reducer<Text, DoubleWritable, Text, MedStdDevWritable> {

	MedStdDevWritable result = new MedStdDevWritable();
	ArrayList<Double> ratingList = new ArrayList<Double>();

	@Override
	protected void reduce(Text arg0, Iterable<DoubleWritable> arg1,
			Reducer<Text, DoubleWritable, Text, MedStdDevWritable>.Context arg2)
			throws IOException, InterruptedException {

		int counter = 0;
		Double sum = 0.0;
		for (DoubleWritable val : arg1) {
			ratingList.add(val.get());
			counter++;
			sum += val.get();
		}
		Collections.sort(ratingList);
		
		double median;
		if (counter % 2 == 0) {
			median = (ratingList.get(counter / 2 - 1) + ratingList.get(counter / 2 - 1)) / 2;
		} else {
			median = ratingList.get(counter / 2);
		}
		result.setMedian(String.valueOf(median));
		
		double means = sum / counter;
		double sumOfSquare = 0.0;
		for(double d:ratingList) {
			sumOfSquare += (d - means)*(d - means);
		}
		double sdtDev = Math.sqrt(sumOfSquare/(counter-1));
		result.setStdDev(String.valueOf(sdtDev));
		
		arg2.write(arg0, result);
	}

}
