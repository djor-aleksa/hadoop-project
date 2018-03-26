package passengersPerHour;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PPHMapper extends Mapper<LongWritable, Text, Text, IntWritable>{


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) {
            return;
        }

        String row = value.toString();

        String outKey = row.split(",")[5].split(" ")[1].split(":")[0]; //extracting hours
        String outVal = row.split(",")[7]; // extracting number of passengers

        context.write(new Text(outKey),new IntWritable(Integer.valueOf(outVal)));
    }
}
