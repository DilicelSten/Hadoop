package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.io.IntWritable;




public class UserVinfo {
	//向HDFS上传本地文件
	public static void uploadInputFile(String localFile) throws IOException{
		Configuration conf = new Configuration();
		String hdfsPath = "hdfs://localhost:9000/";
		String hdfsInput = "hdfs://localhost:9000/user/hadoop/input";
		//在hdfs下新建文件夹
//		Configuration conf = new Configuration();
		FileSystem file = FileSystem.get(conf);
		 file.mkdirs(new Path("/user/hadoop/input"));
		 
       FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.copyFromLocalFile(new Path(localFile), new Path(hdfsInput));
		fs.close();
		System.out.println("已经上传到input文件夹啦");
	}
	
	//创建文件夹（不适用于hdfs即hadoop的文件系统）
//	public static boolean mkDirectory(String path) {  
//        File file = null;  
//        try {  
//            file = new File(path);  
//            if (!file .exists()  && !file .isDirectory()) {  
//                return file.mkdirs();  
//            }  
//            else{  
//                return false;  
//            }  
//        } catch (Exception e) {  
//        } finally {  
//            file = null;  
//        }  
//        return false;  
//    }  
//	
//将output文件下载到本地
	public static void getOutput(String outputfile) throws IOException{
		String remoteFile = "hdfs://localhost:9000/user/hadoop/output/part-r-00000";
		File file=new File(remoteFile);  
		if(file.exists())
		{
		file.renameTo(new File(outputfile));
		}
		Path path = new Path(remoteFile);
		Configuration conf = new Configuration();
		String hdfsPath = "hdfs://localhost:9000/";
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.copyToLocalFile(path, new Path(outputfile));
		System.out.println("已经将文件保留到本地文件");
		fs.close();
	}

//删除HDFS中的output文件
	public static void deleteOutput() throws IOException{
		Configuration conf = new Configuration();
		String hdfsOutput = "hdfs://localhost:9000/user/hadoop/output";
		String hdfsPath = "hdfs://localhost:9000/";
		Path path = new Path(hdfsOutput);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
		fs.deleteOnExit(path);
		fs.close();
		System.out.println("output文件已删除");
	}
	
	//删除HDFS中的input文件
		public static void deleteInput() throws IOException{
			Configuration conf = new Configuration();
			String hdfsInput = "hdfs://localhost:9000/user/hadoop/input";
			String hdfsPath = "hdfs://localhost:9000/";
			Path path = new Path(hdfsInput);
			FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
			fs.deleteOnExit(path);
			fs.close();
			System.out.println("input文件已删除");
		}
		
	//从关注的人中获取大Ｖ及其简介
	public static String[] getUserAndInfo(String block){
		 String user_regex = "<div class=\"info_name.*?<a.*?>(.*?)</a>.*?<a.*?微博个人认证.*?</a>";
         Pattern pattern1= Pattern.compile(user_regex);
         Matcher matcher1 = pattern1.matcher(block);
         String user = null;
         while(matcher1.find()){
         	user = matcher1.group(1);
         	if(user != null){
         		String intro_regex ="<div class=\"info_intro\"><span>(.*?)</span></div>";
                 Pattern pattern2= Pattern.compile(intro_regex);
                 Matcher matcher2 = pattern2.matcher(block);
                 if(matcher2.find()){
                    String info = matcher2.group(1);
                    String value = user + "`" + info;
                    String[] userAndInfo = value.split("`");
                    return userAndInfo;
                 }
         	}
         }
         return null;
	}
	
//创建Mapper类和Reducer类
	public static class UserVMapper extends Mapper<Object, Text, Text, Text>{

        private Text userV = new Text();
        private Text intro = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String html = value.toString();
//            String user_regex ="05050006_\">(.*?)</a>.*? title=\"微博个人" ;
//            String intro_regex ="lass=\"W_icon icon_approve\">.*?info_intro\"><span>(.*?)</span></div>";
//            String user_regex ="微博个人" ;
            String block_regex = "<li class=\"follow_item.*?</li>";
            Pattern pattern = Pattern.compile(block_regex);
            Matcher matcher = pattern.matcher(html);
            while(matcher.find()){
            	String block = matcher.group();
            	String[] userAndInfo = getUserAndInfo(block);
            	if(userAndInfo != null){
            		String user = userAndInfo[0];
            		String info = userAndInfo[1];
            		 userV.set(user);
                     intro.set(info);
                     context.write(userV, intro);
            	}     	
            }
        }
    }

	public static class UserVReducer extends Reducer<Text, Text, Text, Text>{

        private Text value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        	String result = "";
        	for(Text text: values){
            	String one = new String(text.toString());//之前用getByte不行
            	result += one;
            }
        	value.set(result);
        	context.write(key, value);
        }
    }
	
//执行mapReduce程序
	public static void runMapReduce(String [] args) throws Exception{
		Configuration conf = new Configuration();
		String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: wordcount<in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UserV");
		job.setJarByClass(UserVinfo.class);
		job.setMapperClass(UserVMapper.class);
		job.setCombinerClass(UserVReducer.class);
		job.setReducerClass(UserVReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if(job.waitForCompletion(true)==true){
			System.out.println("mapReduce 执行完毕！");
		}
//		System.exit(0);
	}
	
	public static void rewriteFile(String path) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(new File (path)));
		String content = "";
		String line;
		while((line = br.readLine()) != null){
			content += line;
		}
		br.close();
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(path)));
		bw.append(content);
		bw.close();
	}
	
	public static void main(String[] args) throws Exception{
		String localFile = "/home/hadoop/Mapreduce测试";
		String downloadFile = "/home/hadoop/result";
		File[] dir = new File(localFile).listFiles();
		for(File file:dir){
			System.out.println(file.getName());
			String inputFile = localFile + "/" + file.getName();
			 File[] input = new File(inputFile).listFiles();
			 for(File txt: input){
				 String txtPath = inputFile + "/" + txt.getName();
				 rewriteFile(txtPath);
				 //上传文件至input文件夹中
				 uploadInputFile(txtPath);
			 }
			 //进行mapreduce操作
			 runMapReduce(args);
			 //将output文件进行改名字并下载
			 getOutput(downloadFile+"/" + file.getName()+".txt");
			 //将output文件夹删除
			 deleteOutput();
			 //将input文件夹删除
			 deleteInput();
//			 break;
		}
		System.out.print("报告短短，一切正常进行完毕！");
}
}