
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

class Pair { //to implement PriorityQueue
	String key;
	int value;

	Pair(String key, int value) {
		this.key = key;
		this.value = value;
	}
}

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		int threashold;
		// get the threashold parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20);
		}

		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
			//this is cool\t20
			String line = value.toString().trim();
			
			String[] wordsPlusCount = line.split("\t");
			if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);
			
			if(count < threashold) {
				return;
			}
			
			//this is --> cool = 20
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < words.length-1; i++) {
				sb.append(words[i]).append(" ");
			}
			String outputKey = sb.toString().trim();
			String outputValue = words[words.length - 1];
			
			if(!((outputKey == null) || (outputKey.length() <1))) {
				context.write(new Text(outputKey), new Text(outputValue + "=" + count));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

		public Comparator<Pair> pairComparator = new Comparator<Pair> () {
			public int compare(Pair left,Pair right) {
				if (left.value != right.value) {
					return left.value - right.value;
				}
				return right.key.compareTo(left.key);
			}
		};
		int n;
		// get the n parameter from the configuration
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {




			HashMap<String, Integer> counter = new HashMap<>(); //create a hashtable to contain the pairs
			for (Text val:values) {
				String curValue = val.toString().trim();
				String word = curValue.split("=")[0].trim();
				int count = Integer.parseInt(curValue.split("=")[1].trim());
				if (counter.containsKey(word)) {
					//if already in the table, do nothing
				}
				else {
					counter.put(word,count);
				}
			}

			PriorityQueue<Pair> Q = new PriorityQueue<Pair>(n,pairComparator);
			for (String word : counter.keySet()) {
				Pair peak = Q.peek();
				Pair newPair = new Pair(word, counter.get(word));
				if (Q.size() < n) {
					Q.add(newPair);
				} else if (pairComparator.compare(newPair, peak) > 0) {
					Q.poll();
					Q.add(new Pair(word, counter.get(word)));
				}
			}

			List<Pair> resultList = new ArrayList<Pair>();
			int index  = 0;
			while (!Q.isEmpty()) {
				resultList.add(Q.poll());
			}

			for (int i = 0; i < resultList.size(); i++) {
				Pair listElement = resultList.get(i);
				context.write(new DBOutputWritable(key.toString(), listElement.key, listElement.value),NullWritable.get());
			}

		}
	}
}
