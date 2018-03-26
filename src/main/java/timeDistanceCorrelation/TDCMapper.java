package timeDistanceCorrelation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class TDCMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) {
            return;
        }
        String row = value.toString();
        String outKey = row.split(",")[5].split(" ")[0];
        String tripDistance = row.split(",")[9];

        context.write(new Text(outKey), new FloatWritable(Float.valueOf(tripDistance)));

    }

}
