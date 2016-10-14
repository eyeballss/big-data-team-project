package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class hadoop extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new hadoop(),
                args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(hadoop.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

    public static class Map extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] token = value.toString().split("\t");
            
            String user="";
            String date="";
            double velocity=0.0;
            
            try{
            user = token[0];
            date = token[1];
            if(token[3].equals("NaN")) velocity=10.0;
            else velocity = Double.valueOf(token[3]).doubleValue();
            } catch(OutOfRangeException e){
            	return;
            }catch(Exception e){
            	return;
            }
            
            if(velocity<4.0){
                double mKal=Kalori(68.6, 174.0, velocity); //man's kal
                double wKal=Kalori(56.5, 160.5, velocity); //woman's kal
                context.write(new Text("m"+user+'\t'+date), new DoubleWritable(mKal));
                context.write(new Text("w"+user+'\t'+date), new DoubleWritable(wKal));            	
            }
            
        }
    }
    
    static double Kalori(double weight, double height, double vel){
    	return (weight*5.5)*((vel*10/720)/(((height*0.37)+(height-100))/2));
    }
    
    public static class Reduce extends
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        	double sum=0;
        	for(DoubleWritable d: values) {
                sum+=d.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }
}