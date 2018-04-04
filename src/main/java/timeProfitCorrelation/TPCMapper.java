package timeProfitCorrelation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TPCMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(key.get() == 0) {
            return;
        }
        String row = value.toString();
        String outKey = row.split(",")[3].split(" ")[0];
        String totalAmount = row.split(",")[10];

        context.write(new Text(outKey), new FloatWritable(Float.valueOf(totalAmount)));
    }
}
