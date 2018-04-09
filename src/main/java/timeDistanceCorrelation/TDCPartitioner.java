package timeDistanceCorrelation;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TDCPartitioner extends Partitioner<Text, FloatWritable>{

    @Override
    public int getPartition(Text key, FloatWritable value, int i) {
        return 0;
    }
}
