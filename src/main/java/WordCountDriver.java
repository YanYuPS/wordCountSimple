import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//主类，指定job参数并提交job
public class WordCountDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //jar包位置
        job.setJarByClass(WordCountDriver.class);

        //指定mapper类和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //map阶段，输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //数据读取组件 输出组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //要处理的所有文件,所在文件夹路径
        FileInputFormat.setInputPaths(job, new Path("/Users/rainbow/Desktop/wordin"));

        //处理结果存放文件夹路径（如已存在，需删除）
        FileOutputFormat.setOutputPath(job, new Path("/Users/rainbow/Desktop/wordout"));

        //向yarn集群提交job
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}