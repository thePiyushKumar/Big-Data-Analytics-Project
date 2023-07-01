
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Hospital2Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	protected void map (LongWritable offset, Text iso_code, Context con) throws IOException, InterruptedException {
		String lines[] = iso_code.toString().split(",");
		float value = Float.parseFloat(lines[4]);	
		String iso = lines[1];
		Text output_key = new Text(iso);
		FloatWritable output_value = new FloatWritable(value);
		con.write(output_key, output_value);
			
	}

}