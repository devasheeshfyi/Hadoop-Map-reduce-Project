import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import java.io.IOException;

public class MonthRevenueRanking {
    public static class MonthRevenueMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        IntWritable yearMonth = new IntWritable();
        DoubleWritable revenue = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length == 4) {
                int arrivalYear = Integer.parseInt(columns[0]);
                int arrivalMonth = Integer.parseInt(columns[1]);
                double totalRevenue = Double.parseDouble(columns[2]);

                yearMonth.set(arrivalYear * 100 + arrivalMonth);
                revenue.set(totalRevenue);

                context.write(yearMonth, revenue);
            }
        }
    }

    public static class MonthRevenueReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        TreeMap<Double, IntWritable> revenueMap;

        protected void setup(Context context) {
            revenueMap = new TreeMap<>(Collections.reverseOrder());
        }

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Double totalRevenue = 0.0;
            for (DoubleWritable value : values) {
                totalRevenue += value.get();
            }
            revenueMap.put(totalRevenue, new IntWritable(key.get()));
            totalRevenue = 0.0;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, IntWritable> entry : revenueMap.entrySet()) {
                double revenue = entry.getKey();
                IntWritable month = entry.getValue();
                context.write(month, new DoubleWritable(revenue));
            }
        }
    }

    public class ECSeasonMap extends Mapper<Object, Text, Text, DoubleWritable> {
        DoubleWritable revenue = new DoubleWritable();
        Text season = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] columns = value.toString().split(",");
            if (columns.length == 4){
                int arrivalMonth = Integer.parseInt(columns[1]);
                double totalRevenue = Double.parseDouble(columns[2]);
                revenue.set(totalRevenue);
                if(arrivalMonth>=1 && arrivalMonth<=3){
                    String currSeason = "Winter";
                    season.set(currSeason);
                }else if(arrivalMonth>=4 && arrivalMonth<=6){
                    String currSeason = "Spring";
                    season.set(currSeason);
                }else if(arrivalMonth>=7 && arrivalMonth<=8){
                    String currSeason = "Summer";
                    season.set(currSeason);
                }else{
                    String currSeason = "Fall";
                    season.set(currSeason);
                }
                context.write(season, revenue);
            }
        }
    }

    public class ECSeasonReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        TreeMap<Double, Text> revenueMap;

        protected void setup(Context context) {
            revenueMap = new TreeMap<>(Collections.reverseOrder());
        }

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            Double totalRevenue = 0.0;
            for (DoubleWritable value : values) {
                totalRevenue += value.get();
            }
            revenueMap.put(totalRevenue, new Text(key));
            totalRevenue = 0.0;
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, Text> entry : revenueMap.entrySet()) {
                double revenue = entry.getKey();
                Text season = entry.getValue();
                context.write(season, new DoubleWritable(revenue));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Month Revenue Ranking");

        job.setJarByClass(MonthRevenueRanking.class);
        job.setMapperClass(MonthRevenueMapper.class);
        job.setReducerClass(MonthRevenueReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


        Job jobECS = Job.getInstance(conf, "Season-RevenueRanking");

        jobECS.setJarByClass(MonthRevenueRanking.class);
        jobECS.setMapperClass(ECSeasonMap.class);
        jobECS.setReducerClass(ECSeasonReduce.class);
        jobECS.setOutputKeyClass(Text.class);
        jobECS.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(jobECS, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobECS, new Path(args[1]));
        System.exit(jobECS.waitForCompletion(true) ? 0 : 1);


    }
}
