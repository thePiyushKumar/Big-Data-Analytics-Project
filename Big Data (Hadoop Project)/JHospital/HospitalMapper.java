import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class HospitalMapper extends Mapper<LongWritable, Text, Text, Text> {
	protected void map (LongWritable offset, Text indicator, Context con) throws IOException, InterruptedException {
		String lines[] = indicator.toString().split(",");	
		String entity = lines[0];
		String indicators = lines[3];
		Text output_key = new Text(entity);
		Text output_value = new Text(indicators);
		con.write(output_key, output_value);
			
	}

}