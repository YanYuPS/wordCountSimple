import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//输入key（map的输出key：单词），输入value（map的输出value：单词个数），输出key（单词），输出value（单词个数）
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    //ReduceTask调用reduce方法：会先将接收到的kv对按照key分组（key相同为一组）；然后每组key调用一次reduce方法
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {

        int count =0;
        for(IntWritable v :values){
            count += v.get();
        }
        context.write(key, new IntWritable(count));
    }
}