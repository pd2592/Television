package television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import com.amazonaws.services.simpleworkflow.flow.worker.SynchronousActivityTaskPoller; 

public class TelevisionMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	Text outkey = new Text();
	Text outval = new Text();


		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
			String[] array = value.toString().split("\\|");
			String out = array[0];
			String brand=array[1];
			if(brand.contains("NA") || out.contains("NA"))
			{
				outkey.set("IGN");
				outval.set(value);
				context.write(outval, outkey);
			}
			else{
			
				outval.set("PRO");
				outkey.set(value);
				context.write(outkey, outval);
			}
			

			
		}
	}
