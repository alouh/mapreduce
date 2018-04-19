package temperature;

import hdfs.HdfsCommand;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Author: JF Han
 * @Date: Created in 17:23 2018/4/18
 * @Desc: 获取年度最高温度主类
 */
public class MaxTemperature {

    private final static String HDFS_URL = "hdfs://Master:9000";

    private final static String FILE_PATH = "E:/ftp/weather";

    public static void main(String... args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("Max temperature");

        String inputPath = HDFS_URL + "/user/mapreduce/temperature";
        String outputPath = inputPath + "/output";

        HdfsCommand hdfsCommand = new HdfsCommand(HDFS_URL, job.getConfiguration());
        hdfsCommand.rmr(inputPath);
        hdfsCommand.mkdirs(inputPath);
        hdfsCommand.copyFile(FILE_PATH + "/1901", inputPath);
        hdfsCommand.copyFile(FILE_PATH + "/1902", inputPath);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
