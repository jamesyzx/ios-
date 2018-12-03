package list;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class MedStdDevWritable implements Writable,WritableComparable<MedStdDevWritable>{
	String median;
	String stdDev;
	
	public MedStdDevWritable() {
		super();
	}

	public MedStdDevWritable(String median, String stdDev) {
		super();
		this.median = median;
		this.stdDev = stdDev;
	}

	public String getMedian() {
		return median;
	}

	public void setMedian(String median) {
		this.median = median;
	}

	public String getStdDev() {
		return stdDev;
	}

	public void setStdDev(String stdDev) {
		this.stdDev = stdDev;
	}

	public int compareTo(MedStdDevWritable o) {
		// TODO Auto-generated method stub
		int result = median.compareTo(median);
		if(result == 0)
			result = stdDev.compareTo(stdDev);
		return result;
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(median);
		out.writeUTF(stdDev);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		median = in.readUTF();
		stdDev = in.readUTF();
	}

	@Override
	public String toString() {
		return "[median=" + median + ", stdDev=" + stdDev + "]";
	}
	
}
