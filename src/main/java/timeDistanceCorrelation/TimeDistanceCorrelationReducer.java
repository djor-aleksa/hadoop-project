package timeDistanceCorrelation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TimeDistanceCorrelationReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private String longestTripsDay="";
    private Float longestAverageTripDistance = 0.0f;
    @Override
    public void reduce(Text key, Iterable<FloatWritable> value, Context context) throws IOException, InterruptedException {
        Float tripDistanceSum = 0.0f;
        Integer count = 0;

        for(FloatWritable distance : value) {
            tripDistanceSum = tripDistanceSum + distance.get();
            count++;
        }

        Float averageTripDistance = tripDistanceSum/count;

        if(averageTripDistance > longestAverageTripDistance) {
            longestAverageTripDistance = averageTripDistance;
            longestTripsDay = key.toString();
        }
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(longestTripsDay), new FloatWritable(longestAverageTripDistance));
    }
}
