package timeProfitCorrelation;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TPCReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
    private float maxFareAmount = 0;
    private Text maxFareDay = new Text("");

    @Override
    public void reduce(Text key, Iterable<FloatWritable> value, Context context) {
        float sum = 0;

        for(FloatWritable v : value) {
            sum += v.get();
        }

        if(sum > maxFareAmount) {
            maxFareAmount = sum;
            maxFareDay = key;
        }
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxFareDay, new FloatWritable(maxFareAmount));
    }
}
