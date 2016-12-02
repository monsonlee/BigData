package mx1202.wla1;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MR_WLA用于网站日志分析
 *
 * @className MR_WLA
 * @author mxlee
 * @email imxlee@foxmail.com
 * @date 2016年12月2日
 */
public class MR_WLA extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new MR_WLA(), args);
	}

	public int run(String[] args) throws Exception {
		String jobName = "wla_baidu";

		String inputPath = args[0];
		String outputPath = args[1];
		Path path = new Path(outputPath);
		// 删除输出目录
		path.getFileSystem(getConf()).delete(path, true);

		// 1、把所有代码组织到类似于Topology的类中
		Job job = Job.getInstance(getConf(), jobName);

		// 2、一定要打包运行，必须写下面一行代码
		job.setJarByClass(MR_WLA.class);

		// 3、指定输入的hdfs
		FileInputFormat.setInputPaths(job, inputPath);

		// 4、指定map类
		job.setMapperClass(WLA_Mapper.class);

		// 5、指定map输出的<key,value>的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 6、指定reduce类
		job.setReducerClass(WLA_Reducer.class);

		// 7、指定reduce输出的<key,value>的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 8、指定输出的hdfs
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * WLA_Mapper用于网站日志分组
	 *
	 * @className WLA_Mapper
	 * @author mxlee
	 * @email imxlee@foxmail.com
	 * @date 2016年12月2日
	 */
	public static class WLA_Mapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// 格式[2016-11-29 00:02:07 INFO ]
			// (cn.baidu.core.inteceptor.LogInteceptor:55) - [0 183.136.190.51
			// null http://www.baidu.cn/payment]
			String log = value.toString();// 网站访问日志
			String str = "(cn.baidu.core.inteceptor.LogInteceptor:55)";
			String baseUrl = "http://www.baidu.cn/";
			int len = str.length();
			int urlLen = baseUrl.length();
			if (log.indexOf(str) != -1) {
				String[] log1 = log.split(str);
				// 分析第一段[2016-11-29 00:29:58 INFO
				String visitTime = log1[0].substring(1, 20);// 获取访问时间
				// 分析第二段112.90.82.196 null
				// http://www.baidu.cn/course/jobOffline]
				String[] split2 = log1[1].split("\t");
				String ip = split2[1];// 获取ip
				String url = split2[3];// 获取网址
				String subUrl = "http://www.baidu.cn";
				if (url.length() - 1 > urlLen) {
					subUrl = url.substring(urlLen, url.length() - 1);
				}
				String result = visitTime + "," + subUrl;
				context.write(new Text(ip), new Text(result));
			}
		}

	}

	/**
	 * WLA_Reducer用于处理分组后的数据
	 *
	 * @className WLA_Reducer
	 * @author mxlee
	 * @email imxlee@foxmail.com
	 * @date 2016年12月2日
	 */
	public static class WLA_Reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			long firstTime = Long.MAX_VALUE;// 首次访问时间
			String startTime = null;
			String endTime = null;
			long lastTime = Long.MIN_VALUE;
			String firstPage = null;// 首次访问页面
			String lastPage = null;
			int count = 0;// 访问页面次数

			for (Text value : values) {
				count++;
				String[] split = value.toString().split(",");

				if (TimeUtil.transDate(split[0]) < firstTime) {
					firstTime = TimeUtil.transDate(split[0]);// yyyy-MM-dd
																// HH:mm:ss
					startTime = split[0].substring(11, 19);
					firstPage = split[1];
				}

				if (TimeUtil.transDate(split[0]) > lastTime) {
					lastTime = TimeUtil.transDate(split[0]);
					endTime = split[0].substring(11, 19);
					lastPage = split[1];
				}

			} // end for

			long time = 0;
			if ((lastTime - firstTime) % (1000 * 60) > 0) {
				time = (lastTime - firstTime) / (1000 * 60) + 1;
			} else {
				time = (lastTime - firstTime) / (1000 * 60);
			}
			String result = startTime + "\t" + firstPage + "\t" + endTime + "\t" + lastPage + "\t" + count + "\t" + time
					+ "分钟";
			context.write(key, new Text(result));

		}// end reduce

	}// end class

}
