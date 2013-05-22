import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: dasheng
 * Date: 13-5-22
 * Time: 上午11:59
 * To change this template use File | Settings | File Templates.
 */
public class SqlMain {
    public static class SqlMap extends Mapper<Object,Text,Text,Text> {

        Text one = new Text(),two = new Text();

        public void map (Object key , Text value , org.apache.hadoop.mapreduce.Mapper.Context context)
                throws IOException ,InterruptedException{
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line,",");
            String s1,s2;
            s1 = st.nextToken();s2 = st.nextToken();
            if(s2.compareTo("date") != 0) {
                one.set(s1);
                two.set(s2);
                context.write(two,one);
            }
        }
    }
    public static class SqlReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
            HashSet<Text> set = new HashSet<Text>();
            int count = 0;
            for(Text val : values)
            {
                count++;
                set.add(val);
            }
            context.write(key, new Text(count + " " + set.size())) ;

        }
    }
    public static void main(String args []) throws Exception {
        Configuration cf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(cf, args).getRemainingArgs();

        Job job = new Job(cf,"sqlmain");
        job.setJarByClass(SqlMain.class);

        job.setMapperClass(SqlMap.class);
        //job.setCombinerClass(SqlReducer.class);
        job.setReducerClass(SqlReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
