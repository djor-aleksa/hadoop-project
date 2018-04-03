package timeProfiltCorrelation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TPCReducer extends Reducer<Text, Float, Text, Float>{
    private float maxFareAmount = 0;
    private Text maxFareDay = new Text("");

    @Override
    public void reduce(Text key, Iterable<Float> value, Context context) {
        float sum = 0;

        for(Float v : value) {
            sum += v;
        }

        if(sum > maxFareAmount) {
            maxFareAmount = sum;
            maxFareDay = key;
        }
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(maxFareDay, maxFareAmount);
    }
}
