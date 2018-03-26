package passengersPerHour;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PPHReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for(IntWritable v : values)
        {
           sum = sum + v.get();
           count++;
        }
        context.write(key, new FloatWritable(sum/(float)count));
    }
}
