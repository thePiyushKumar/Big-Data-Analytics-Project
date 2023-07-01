import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class HospitalReducer extends Reducer<Text, Text, Text, Text> {
	

	public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
		
		for(Text val : values) {
	
		Text output_key = new Text( key  );
		Text output_value = new Text( val  );
		con.write(output_key, output_value);
		}
		
		
		
	}
	
}
