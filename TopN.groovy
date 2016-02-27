import java.io.IOException;

import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser

public class TopN {
    public static class TopNMapper extends Mapper<Object, Text, NullWritable, Text>{

        def partialResults = [:] as TreeMap

        public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException{
	    Configuration conf = context?.getConfiguration()
	    int N = conf?.getInt("N",10)
            partialResults?.put(Long.parseLong(value?.toString()), new Text(value))
            if(partialResults?.size() > N){
                partialResults?.remove(partialResults?.firstKey())
            }
        }

        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException{
            for(Text text : partialResults?.values()){
                context?.write(NullWritable?.get(),new Text(text))
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        def reducedResults = [:] as TreeMap

        protected void reduce (NullWritable key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException{
            Configuration conf = context?.getConfiguration()
            int N = conf?.getInt("N",10)
            for (Text text : values){
                reducedResults?.put(Long.parseLong(text?.toString()),new Text(text))
                if(reducedResults?.size() > N){
                    reducedResults?.remove(reducedResults?.firstKey())
                }
            }
            for(Text text : reducedResults?.descendingMap()?.values()){
                context?.write(NullWritable?.get(),text)
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration()
        String [] others = new GenericOptionsParser(configuration,args).getRemainingArgs()
        if(others.length < 2 || others.length > 3){
            System.err.println("Usage: TopN <file-in> <folder-out> <N>")
            System.exit(2)
        }
	if(others.length == 3){
	    configuration.setInt("N", others[2]?.toInteger())
	}
        Job job = Job.getInstance(configuration,"Top N numbers given a file")
        job.setJarByClass(TopN.class)
        job.setMapperClass(TopNMapper.class)
        job.setReducerClass(TopNReducer.class)
        job.setNumReduceTasks(1)
        job.setOutputKeyClass(NullWritable.class)
        job.setOutputValueClass(Text.class)
        FileInputFormat.addInputPath(job, new Path(others[0]))
        FileOutputFormat.setOutputPath(job, new Path(others[1]))
        System.exit(job.waitForCompletion(true) ? 0 : 1)
    }
}
