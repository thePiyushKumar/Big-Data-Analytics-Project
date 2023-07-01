import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HospitalMain {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		Job j = new Job(c, "indicator");
		j.setJar("Hospital.jar");
		j.setJarByClass(HospitalMain.class);
		j.setMapperClass(HospitalMapper.class);
		j.setReducerClass(HospitalReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)? 0 : 1);
    }
}
