package timeProfiltCorrelation;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPCPartitioner  extends Partitioner<Text, FloatWritable>{

    @Override
    public int getPartition(Text text, FloatWritable floatWritable, int i) {
        return 0;
    }

}
