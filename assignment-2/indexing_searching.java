import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class indexing_searching extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new indexing_searching(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);

    Configuration conf = getConf();
    Job job = new Job(conf, this.getClass().toString());

    FileInputFormat.setInputPaths(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setJobName("indexing_searching");
    job.setJarByClass(indexing_searching.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value,
                    Mapper.Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }
public class Node
{
    int PhraseId = -1;
    Dictionary<String, Node> Children = new Dictionary<String, Node>();

    public Node() { }
    public Node(int id)
    {
        PhraseId = id;
    }
}
void addPhrase(ref Node root, String phrase, int phraseId)
{
    Node node = root;
    String[] words = phrase.Split ();
    for (int i = 0; i < words.Length; ++i)
    {
        if (node.Children.ContainsKey(words[i]) == false)
            node.Children.Add(words[i], new Node());
        node = node.Children[words[i]];
        if (i == words.Length - 1)
            node.PhraseId = phraseId;
    }
}
void findPhrases(ref Node root, String textBody)
{
    Node node = root;
    List<int> foundPhrases = new List<int>();
    String[] words = textBody.Split ();
    for (int i = 0; i < words.Length;)
    {
        if (node.Children.ContainsKey(words[i]))
        {
            node = node.Children[words[i]];
            ++i;
        }
        else
        {
            if (node.PhraseId != -1)
                foundPhrases.Add(node.PhraseId);


            if (node == root)
            {
                ++i;
            }
            else
            {
                node = root;
            }
                
        }
    }
    if (node.PhraseId != -1)
        foundPhrases.Add(node.PhraseId);
}
  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }

      context.write(key, new IntWritable(sum));
    }
  }

}