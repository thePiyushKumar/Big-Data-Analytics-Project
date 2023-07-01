import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Hospital3Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		Float min = 0f, max = 0f;
		ArrayList<Float> arr = new ArrayList<Float>();
		public void reduce(Text key, Iterable<FloatWritable> values, Context con) throws IOException, InterruptedException {
			
			for(FloatWritable val : values) {
				arr.add(val.get());
			}
			
			min=Collections.min(arr);
			max=Collections.max(arr);
			arr.clear();
			Text output_key = new Text(key + " Minimum : " + min + " Maximum : " +max);
			con.write(output_key, null);
		}
		
}