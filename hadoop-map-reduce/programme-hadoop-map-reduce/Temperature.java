package hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Programme Hadoop pour calculer la température moyenne annuelle
 * par région et pays à partir du fichier city_temperature.csv.
 *
 * Format de sortie : Region,Country,Year,AverageTemperature
 */
public class Temperature extends Configured implements Tool {

    public static class TemperatureMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private Text outputKey = new Text();
        private FloatWritable outputValue = new FloatWritable();

        @Override
        protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("Region")) return;

            String[] data = line.split(",");

            if (data.length < 7) return;
   
            // verification de la validité des dates
            if (data[6] != null && !data[6].isEmpty() && !"0".equals(data[6]) &&
                    data[4] != null && !data[4].isEmpty() && !"0".equals(data[4]) &&
                    data[5] != null && !data[5].isEmpty() && !"0".equals(data[5])) {
                try {
                    String region = data[0].trim();
                    String country = data[1].trim();
                    String year = data[6].trim();
                    if (Float.parseFloat(data[7].trim()) < 0) {
                        System.out.println("Temperature is negative, skipping: " + data[7].trim());
                        return; 
                        
                    }
                    float temperature = Float.parseFloat(data[7].trim());

                    String composedKey = region + "," + country + "," + year+ ",";
                    outputKey.set(composedKey);
                    outputValue.set(temperature);
                    context.write(outputKey, outputValue);

                } catch (Exception e) {

                }
            }else {
                return;
            }
        }
    }

    public static class TemperatureReducer extends Reducer<Text, FloatWritable, Text, Text> {
        private static boolean headerWritten = false;

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
            if (!headerWritten) {
                context.write(new Text("Region,Country,Year,AverageTemperature"), new Text(""));
                headerWritten = true;
            }
            float sum = 0;
            int count = 0;

            for (FloatWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                float avg = sum / count;
                context.write(key, new Text(String.valueOf(avg))); 
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Temperature <input path> <output path>");
            return -1;
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Temperature.class);
        job.setJobName("Average Annual Temperature by Region and Country");

        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Temperature(), args);
        System.exit(exitCode);
    }
}
