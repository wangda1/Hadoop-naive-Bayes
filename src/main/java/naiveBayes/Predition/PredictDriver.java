package naiveBayes.Predition;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PredictDriver {
	
	public static void main(String[] args) throws NumberFormatException, IOException, ClassNotFoundException, InterruptedException {
		
		args = new String[] {"C:/Users/wangc/Desktop/naiveBayes/ideaPrj/NBCorpus/Test", "C:/Users/wangc/Desktop/naiveBayes/ideaPrj/NBCorpus/output_class"};
		
		// 1 获取job信息
		Prediction prediction = new Prediction();
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "prediction");
		
		// 2 获取jar包位置
		job.setJarByClass(Prediction.class);
		
		// 3 关联自定义的mapper和reducer
		job.setMapperClass(PredictMapper.class);
		job.setReducerClass(PredictReducer.class);
		
		// 4 设置自定义的InputFormat类
		job.setInputFormatClass(PredictTestInputFormat.class);	
		
		// 5 设置map输出数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// 6 设置最终输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		
		// 7 设置输入和输出文件路径
		ArrayList<Path> paths = GetPaths(args[0]);
		for(int i=0; i < paths.size(); i++) {
			FileInputFormat.addInputPath(job, paths.get(i));			
		}	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 8 提交代码
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
	
	private static ArrayList<Path> GetPaths(String path) {
		// 获取path路径下所有子文件夹路径
		ArrayList<Path> paths = new ArrayList<Path>();
		File file = new File(path);
		// 如果这个路径是文件夹
		if (file.isDirectory()) {
			// 获取路径下的所有文件
			File[] files = file.listFiles();
			for (int i=0; i<files.length; i++) {
				// 如果还是文件夹
				if (files[i].isDirectory()) {
					// 将其加入路径列表
					paths.add(new Path(files[i].getPath()));
				}
				else {continue;}
			}
		}
		return paths;
	}
}
