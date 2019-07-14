package ru.vvdolgov.stackoverflow.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;


public class StackOverflowMapReduceTool {

    private static class PostMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        private static final SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");

        private static final int currentYear = Integer.valueOf(yearFormat.format(new Date()));
        private static final int previousYear = currentYear - 1;

        private Text outKey = new Text();
        private IntWritable outValue = new IntWritable();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsedRow = SimpleXmlParser.parseXmlRow(value.toString());
            String tags = parsedRow.get("Tags");
            String score = parsedRow.get("Score");
            String views = parsedRow.get("ViewCount");
            String answers = parsedRow.get("AnswerCount");
            String comments = parsedRow.get("CommentCount");

            String lastActivityDateAsString = parsedRow.get("LastActivityDate");
            Date lastActivityDate = null;

            try {
                lastActivityDate = dateFormat.parse(lastActivityDateAsString);
            }
            catch (ParseException e)
            {
                e.printStackTrace();
            }

            if(StringUtils.isNotBlank(tags) && lastActivityDate != null){
                int lastActivityDateYear = Integer.valueOf(yearFormat.format(lastActivityDate));
                if(lastActivityDateYear == currentYear || lastActivityDateYear == previousYear) {
                    String[] tagsAsArray = SimpleXmlParser.parseMultipleAttributeValues(tags);

                    for (String tag : tagsAsArray) {
                        outKey.set(tag);
                        int totalScore = Integer.valueOf(score)
                                + Integer.valueOf(views)
                                + Integer.valueOf(answers)
                                + Integer.valueOf(comments);
                        outValue.set(lastActivityDateYear == currentYear ? totalScore : -totalScore);
                        context.write(outKey, outValue);
                    }
                }
            }
        }
    }

    private static class PostReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static TreeMap<Integer, Text> topNMap = new TreeMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> scores, Context context) throws IOException, InterruptedException {
            int totalScore = 0;
            for(IntWritable score : scores){
                totalScore += score.get();
            }

            topNMap.put(totalScore, key);

            if(topNMap.size()>10){
                topNMap.remove(topNMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Integer key : topNMap.keySet()){
                context.write(topNMap.get(key), new IntWritable(key));
            }
        }
    }


    public static void main(String[] strings) throws Exception {
        String inputFile = strings[0];
        String outputFile = strings[1];


        Job job = Job.getInstance(new Configuration(true), "TOP-10 most popular technology posts");
        job.setJarByClass(StackOverflowMapReduceTool.class);
        job.setMapperClass(PostMapper.class);
        job.setReducerClass(PostReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
