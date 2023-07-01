import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Hospital3Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	protected void map (LongWritable offset, Text date, Context con) throws IOException, InterruptedException {
		
		String lines[] = date.toString().split(",");
		String year = lines[2].substring(6, 10);	
		
		float value = Float.parseFloat(lines[4]);			
		Text output_key = new Text(year);
		FloatWritable output_value = new FloatWritable(value);
		con.write(output_key, output_value);
	
	}
	
}