import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//Map：每个单词记一个1
//输入key（一行文本的偏移量），输入value（一行文本内容），输出key（单词），输出value（单词个数）
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //MapTask调用map方法，每读取一个（keyIn，valueIn），就调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取每一行的文本内容
        String lines = value.toString();
        String[] words = lines.split(" ");

        for (String word :words) {
            context.write(new Text(word), new IntWritable(1));
        }
    }
}