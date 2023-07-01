import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Hospital3Main {
	
	public static void main(String[] args) throws Exception {
		
		Configuration c = new Configuration();
		Job j = new Job(c, "min and max");
		j.setJar("Hospital3.jar");
		j.setJarByClass(Hospital3Main.class);
		j.setMapperClass(Hospital3Mapper.class);
		j.setReducerClass(Hospital3Reducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)? 0 : 1);
	
	}
	
}