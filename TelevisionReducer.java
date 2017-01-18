package television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TelevisionReducer extends Reducer<Text, Text, Text, Text>
{	
	Text outvalue = new Text();
	Text outblank=new Text(" ");
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
	{
		for(Text value:values){
			if(value.toString().contains("PRO"))
			{
				outvalue.set(value);
				context.write(key,outblank);
			}

		}
		
	}
}
