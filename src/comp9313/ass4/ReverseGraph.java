/*
 * COMP9313 Assignment 4, Problem 1
 * Author: Huijun Wu (z5055605)
 * Date: Oct. 17th, 2016
 */
package comp9313.ass4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * Using mapreduce to do reverse graph is quite straightforward. Just exchange the source and dest. or each edge is enough. 
 */

public class ReverseGraph {
	
	//we define a pair class to support the value-to-key design patten which further helps with sorting the nodes in adjacent lists. 
	public static class Pair implements WritableComparable {

		private IntWritable term1;

		private IntWritable term2;

		public IntWritable getTerm1() {
			return term1;
		}

		public IntWritable getTerm2() {
			return term2;
		}

		public Pair() {

		}

		public Pair(IntWritable t1, IntWritable t2) {

			set(t1, t2);

		}

		public void set(IntWritable t1, IntWritable t2) {
			term1 = t1;
			term2 = t2;
		}

		// We have to override the following two functions to implement the
		// serialization and de-serialization of our defined Pair type.

		@Override
		public void readFields(DataInput in) throws IOException {

			term1 = new IntWritable(in.readInt());
			term2 = new IntWritable(in.readInt());
		}

		@Override
		public void write(DataOutput out) throws IOException {

			out.writeInt(term1.get());

			out.writeInt(term2.get());

		}
		//we use the value-to-key design pattern to sort the ascending order for nodes in adjacent list. 
		@Override
		public int compareTo(Object po) {

			Pair p = (Pair) po;

			int ret = term1.get() - p.term1.get();
			if (ret != 0) {

				return ret;
			}

			return term2.get() - p.term2.get();

		}

	}

	public static class ReverseGraphMapper extends Mapper<Object, Text, Pair, IntWritable> {

		private IntWritable source = new IntWritable();

		private IntWritable dest = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

			while (itr.hasMoreTokens()) {

				String nextStr = itr.nextToken();

				String record = new String();

				if (Character.isDigit(nextStr.charAt(0))) {

					StringTokenizer line = new StringTokenizer(nextStr, "\t");

					dest.set(Integer.parseInt(line.nextToken()));

					source.set(Integer.parseInt(line.nextToken()));

					context.write(new Pair(source, dest), dest);

				}

			}
		}
	}
	//we define our own partitioner to ensure that pairs with same term1 are sent to the same reducer. 
	public static class ReverseGraphKeyPartition extends Partitioner<Pair, IntWritable> {

		@Override
		public int getPartition(Pair key, IntWritable value, int numReduceTasks) {

			return (key.getTerm1().hashCode()) % numReduceTasks;

		}

	}

	public static class ReverseGraphReducer extends Reducer<Pair, IntWritable, Text, Text> {
		
		/*
		 * Since we need to guarantee the ascending order in output, we use a LinkedHashMap to keep the ordering provided by 
		 * shuffling process.
		 */
		

		private LinkedHashMap<Integer, ArrayList<Integer>> tmpAdjacentLists = new LinkedHashMap<Integer, ArrayList<Integer>>();

		public void reduce(Pair key, Iterable<IntWritable> values, Context context)

				throws IOException, InterruptedException {

			String adjacentNodes = "";

			boolean isFirst = true;

			int curkey = 0;

			for (IntWritable val : values) {

				if (tmpAdjacentLists.get(key.getTerm1().get()) == null) {

					ArrayList<Integer> tmp = new ArrayList<Integer>();

					tmp.add(val.get());

					tmpAdjacentLists.put(key.getTerm1().get(), tmp);

				} else {

					tmpAdjacentLists.get(key.getTerm1().get()).add(val.get());
				}

			}

		}
		
		//After all pairs for this reducer have been received, we can output the whole ordered adjacentlist for each node.
		public void cleanup(Context context) throws IOException, InterruptedException {

			for (Entry<Integer, ArrayList<Integer>> entry : tmpAdjacentLists.entrySet()) {

				String toEmitAdjacentList = "";

				Iterator itr = entry.getValue().iterator();

				toEmitAdjacentList += itr.next().toString();

				while (itr.hasNext()) {

					toEmitAdjacentList += ("," + itr.next().toString());
				}

				context.write(new Text(entry.getKey().toString()), new Text(toEmitAdjacentList.toString()));

			}

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "GraphEdgeReverse");

		job.setJarByClass(ReverseGraph.class);

		job.setMapperClass(ReverseGraphMapper.class);

		job.setReducerClass(ReverseGraphReducer.class);

		job.setOutputKeyClass(Pair.class);

		job.setPartitionerClass(ReverseGraphKeyPartition.class);

		job.setOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(1);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}