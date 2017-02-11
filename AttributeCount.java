package org.apache.hadoop.examples;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AttributeCount {
	//匹配性别
    public static class GenderMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String regex = "性别:(.*?)<br/>";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(line);
            while(matcher.find()){
                String term = matcher.group(1);
                word.set(term);
                context.write(word, one);
            }
        }
    }
    //匹配地区
    public static class RegionMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String regex = "地区:(.*?)<br/>";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(line);
            while(matcher.find()){
                String term = matcher.group(1);
                word.set(term);
                context.write(word, one);
            }
        }
    }
    //匹配年龄
    public static class AgeMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String regex = "生日:(.*?)-.*?<br/>";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(line);
            while(matcher.find()){
                String term = matcher.group(1);
                word.set(term);
                context.write(getPersonAgeByBirthDate(word), one);
            }
        }
    }
//进行整合
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int sum = 0;
            for(IntWritable val :values){
                sum+= val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
  //删除HDFS中的文件
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
  //将output文件下载到本地
  	public static void getOutput(String outputfile) throws IOException{
  		String remoteFile = "hdfs://localhost:9000/user/hadoop/output/part-r-00000";
  		Path path = new Path(remoteFile);
  		Configuration conf = new Configuration();
  		String hdfsPath = "hdfs://localhost:9000/";
  		FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
  		fs.copyToLocalFile(path, new Path(outputfile));
  		System.out.println("已经将文件保留到本地文件");
  		fs.close();
  	}	
//执行mapReduce
    public static void mapReduce(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2){
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        //性别
        System.out.println("性别统计结果如下：");
        Job job = new Job(conf, "gender count");
        job.setJarByClass(AttributeCount.class);
        job.setMapperClass(GenderMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
       if(job.waitForCompletion(false)==true){
    	   String path = "/home/hadoop/workspace/性别";
    	   getOutput(path);
    	   deleteOutput();
    	   Configuration conf1 = new Configuration();
           String[] otherArgs1 = new GenericOptionsParser(conf1, args).getRemainingArgs();
           if(otherArgs1.length != 2){
               System.err.println("Usage: wordcount <in> <out>");
               System.exit(2);
           }
    	 //地区
           System.out.println("地区统计结果如下：");
           Job job1 = new Job(conf, "region count");
           job1.setJarByClass(AttributeCount.class);
           job1.setMapperClass(RegionMapper.class);
           job1.setCombinerClass(IntSumReducer.class);
           job1.setReducerClass(IntSumReducer.class);
           job1.setOutputKeyClass(Text.class);
           job1.setOutputValueClass(IntWritable.class);
           FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
           FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
         if(job1.waitForCompletion(false)==true){
        	 String path1 = "/home/hadoop/workspace/地区";
        	 getOutput(path1);
        	 deleteOutput();
        	 Configuration conf2 = new Configuration();
             String[] otherArgs2 = new GenericOptionsParser(conf2, args).getRemainingArgs();
             if(otherArgs2.length != 2){
                 System.err.println("Usage: wordcount <in> <out>");
                 System.exit(2);
             }
        	 //年龄
             System.out.println("年龄统计结果如下：");
             Job job2 = new Job(conf, "age count");
             job2.setJarByClass(AttributeCount.class);
             job2.setMapperClass(AgeMapper.class);
             job2.setCombinerClass(IntSumReducer.class);
             job2.setReducerClass(IntSumReducer.class);
             job2.setOutputKeyClass(Text.class);
             job2.setOutputValueClass(IntWritable.class);
             FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
             FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
             if(job2.waitForCompletion(false)==true){
             String path2 = "/home/hadoop/workspace/年龄";
      	    getOutput(path2);
      	    deleteOutput();
      	    System.exit(0);}
             
       }
       }
    }
  //转化年龄
    public static Text getPersonAgeByBirthDate(Text yearofbirth){  
    	String word = yearofbirth.toString();
//        if ("".equals(word) || word == null){  
//            return "";  
//        }  
        //读取当前日期  
        Calendar c = Calendar.getInstance();  
        int year = c.get(Calendar.YEAR);  
//        int month = c.get(Calendar.MONTH)+1;  
//        int day = c.get(Calendar.DATE);  
        //计算年龄  
        int age = year - Integer.parseInt(word.substring(0, 4)) - 1;  
//        if (Integer.parseInt(word.substring(5,7)) < month) {  
//            age++;  
//        } else if (Integer.parseInt(word.substring(5,7))== month && Integer.parseInt(word.substring(8,10)) <= day){  
//            age++;  
//        }  
        Text age1 = new Text();
        age1.set(String.valueOf(age));
        return age1;  
    }  
   

	public static void main(String[] args) throws Exception {
    	mapReduce(args);
    }
}
