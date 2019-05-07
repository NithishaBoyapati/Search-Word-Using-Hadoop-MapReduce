import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class SearchWord {

    //public static ArrayList<String> searchwords = new ArrayList<String>();
    //public static String searchwords[]={"and","the","for"};
    //public static String searchwords[]=new String[3];

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        //public void setup(Context context) throws IOException{
   


        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
                Path filePath = ((FileSplit) context.getInputSplit()).getPath();
                String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                Path pt=new Path("hdfs:/user/snehabaskaran/search/search.txt");//Location of file in HDFS
                FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String searchword[]=br.readLine().split(",");
        

        //Path pt1=new Path("hdfs:/user/snehabaskaran/input/input.txt");//Location of file in HDFS
        //FileSystem fs1 = FileSystem.get(new Configuration());
        //BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));
        //String line;
        //line=br1.readLine();
        //while (line != null){
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            //try{
            while (tokenizer.hasMoreTokens()) {
                String w=tokenizer.nextToken();
                for(String s: searchword)
                {
                    if(s.equalsIgnoreCase(w))
                    {
                        value.set(fileName+" "+w);
                        context.write(value, new IntWritable(1));
                    }
                }
                
           }
           //line=br1.readLine();
       //}
        //}
        //catch(Exception e)
        //{
        //    System.err.println(e);
        //}

        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {
            int sum = 0;
        for (IntWritable val : values) {
        sum += val.get();
            }
       
        context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
    Job job = new Job(conf, "Search Word");
    job.setJarByClass(SearchWord.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}
