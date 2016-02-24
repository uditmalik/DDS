package assignment4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
	
	public static class mymapper extends Mapper<LongWritable,Text,Text,Text>
	{
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String[] parts= value.toString().split(":");
			String tablename=parts[0];
			String joinkey=parts[1];
			String tuple=parts[2];
			
			con.write(new Text(joinkey),new Text(tablename+"::"+tuple));
			System.out.println("joinkey"+joinkey+ " "+"text: "+tablename+"::"+tuple);
		}
		
	}
	
	public static class myreducer extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> values,Context con) throws IOException, InterruptedException
		{
			
			String result="";
			List<String> al=new ArrayList<String>();
			List<String> bl=new ArrayList<String>();
			System.out.println(values.toString());
			al.addAll((List<String>)Arrays.asList(values.toString()));
			bl.addAll((List<String>)Arrays.asList(values.toString()));			
			//System.out.println(al);
			for(String t1:al)
			{
				
				
				for(String t2:bl )
				{
					
					String table1=t1.split("::")[0];
					
					String tuple1=t1.split("::")[1];
					
					String table2=t2.split("::")[0];
					String tuple2=t2.split("::")[1];
					if(table1.equals(table2)==false)
					{
						result=tuple1+","+tuple2;
						
						con.write(key,new Text(result));
					
					}
				}
				bl.remove(0);
				
				
				
			}
		}
	}


	public static void main(String[] args) throws Exception 
	{
		if(args.length !=2)
		{
			System.out.println("Arguments less than 2");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(equijoin.class);
		job.setMapperClass(mymapper.class);
		job.setCombinerClass(myreducer.class);
		job.setReducerClass(myreducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
	
}