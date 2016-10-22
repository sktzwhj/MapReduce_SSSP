/*
 * COMP 9313 Assignment 4, Problem 2
 * Author: Huijun Wu (z5055605)
 * Date: Oct. 19th, 2016
 */

package comp9313.ass4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import comp9313.ass4.ReverseGraph.Pair;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//enum type for counter used in main function. 
enum UpdateCounter {
	UpdateDistance
}

public class SingleSourceSP {

	public static String OUT = "output";

	public static String IN = "input";
	// we define maxDistance as the longest distance which indicates the
	// distance is not updated.
	private static Double maxDistance = 65535.0;

	public static class FormatTransMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

			while (itr.hasMoreTokens()) {

				String[] line = itr.nextToken().split(" ");

				context.write(new Text(line[1]), new Text(line[2] + ":" + line[3]));

			}

		}
	}

	public static class FormatTransReducer extends Reducer<Text, Text, Text, Text> {

		private LinkedHashMap<String, ArrayList<String>> tmpAdjacentLists = new LinkedHashMap<String, ArrayList<String>>();

		public void reduce(Text key, Iterable<Text> values, Context context)

				throws IOException, InterruptedException {

			for (Text val : values) {

				if (tmpAdjacentLists.get(key.toString()) == null) {

					ArrayList<String> tmp = new ArrayList<String>();

					tmp.add(val.toString());

					tmpAdjacentLists.put(key.toString(), tmp);

				} else {
					tmpAdjacentLists.get(key.toString()).add(val.toString());
				}

			}
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

			for (Entry<String, ArrayList<String>> entry : tmpAdjacentLists.entrySet()) {

				String toEmitAdjacentList = "";

				Iterator itr = entry.getValue().iterator();

				toEmitAdjacentList += itr.next().toString();

				while (itr.hasNext()) {

					toEmitAdjacentList += ("," + itr.next().toString());
				}

				if (entry.getKey().equals(context.getConfiguration().get("queryNode"))) {

					context.write(new Text(
							entry.getKey().toString() + " " + "0.0" + "|" + toEmitAdjacentList.toString() + "|Y"),
							new Text());
				} else {
					context.write(new Text(entry.getKey().toString() + " " + maxDistance.toString() + "|"
							+ toEmitAdjacentList.toString() + "|Y"), new Text());

				}

			}

		}

	}

	public static class SPMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

			while (itr.hasMoreTokens()) {

				/*
				 * According to our format transformation, the original input
				 * with format [id, fromNode, toNode, distance] is transformed
				 * into format of [nodeId distance|
				 * adjacentList(nodeId:edgeLength) |updated(Y/N) ]
				 */
				String nodeRecord = itr.nextToken();

				String[] nodeInfo = nodeRecord.split("\\|");

				assert nodeInfo[1].length() > 2;

				Integer nodeId = Integer.parseInt(nodeInfo[0].split(" ")[0]);

				// no matter the shortestDistance for this node have been
				// updated in last run or not, we emit its adjacent list.

				context.write(new IntWritable(nodeId), new Text("A" + nodeInfo[1]));

				Double shortestDistance = Double.parseDouble(nodeInfo[0].split(" ")[1]);

				/*
				 * O for original distance, the reducer can use this to know
				 * whether the shortest distance for a certain node has been
				 * updated.
				 */
				context.write(new IntWritable(nodeId), new Text("O" + shortestDistance.toString()));

				// firstly find out the adjacent nodes of the source node

				String[] adjacentNodes = nodeInfo[1].split(",");

				// very tricky part, there is a tab between key and value in the
				// output file...

				if ((Double.compare(shortestDistance, maxDistance) != 0) && nodeInfo[1].length() > 1) {

					// System.out.println("enter here");

					for (int i = 0; i < adjacentNodes.length; i++) {

						// in order to recover the adjacent list for the node,
						// we also need to emit the adjacent list for nodes.
						// the condition is to filter some exceptions, i.e.,
						// node 240 has no adjacentList.
						if (adjacentNodes[i].split(":")[0].length() > 0) {

							context.write(new IntWritable(Integer.parseInt(adjacentNodes[i].split(":")[0])),
									new Text("U" + new Double(
											shortestDistance + Double.parseDouble(adjacentNodes[i].split(":")[1]))
													.toString()));
						}

					}

				}

			}

		}

	}

	public static class SPReducer extends Reducer<IntWritable, Text, Text, Text> {

		private Counter updateCounter;

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// YOUR JOB: reduce function
			// ... ...
			Double minShortestDistance = maxDistance;

			boolean flagFirst = false;

			boolean update = false;

			String adjacentList = "";

			Double originalDistance = maxDistance;

			for (Text val : values) {

				try {
					if (val.toString().startsWith("O")) {
						// get the original shortest distance in the last run
						// for this node

						originalDistance = Double.parseDouble(val.toString().split("O")[1]);

						if (originalDistance < minShortestDistance) {

							minShortestDistance = originalDistance;
						}
					}

					if (val.toString().startsWith("U")) {
						// get the shortest distances updated in this run
						Double distance = Double.parseDouble(val.toString().split("U")[1]);

						if (distance < minShortestDistance) {

							minShortestDistance = distance;

						}

					}
					// get the adjacent list which needs to be appended in the
					// output.
					if (val.toString().startsWith("A")) {

						if (val.toString().length() > 2) {

							adjacentList = val.toString().split("A")[1];
						} else {
							adjacentList = "";
						}

					}
				} catch (Exception e) {

					e.printStackTrace();
				}

			}

			// it is more straightforward to set the value for update here.
			if (Double.compare(originalDistance, minShortestDistance) != 0) {

				update = true;
				// we use updateCounter to see if there is some update in this
				// run of mapreduce job.
				updateCounter = context.getCounter(UpdateCounter.UpdateDistance);

				updateCounter.increment(1);
			}
			// output

			// the output for the last run and intermediate run are different.
			if (context.getConfiguration().get("JobSeq").equals("Last")) {
				context.write(new Text(context.getConfiguration().get("queryNode") + " " + key.toString() + " "
						+ minShortestDistance.toString()), new Text(""));

			} else {
				context.write(new Text(key.toString() + " " + minShortestDistance.toString() + "|" + adjacentList
						+ ((update == true) ? "|Y" : "|N")), new Text(""));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Double maxDistance = 65535.0;

		IN = args[0];

		System.out.println(IN);

		OUT = args[1];

		/*
		 * It seems that we cannot specify the S3 url as the input for the read
		 * operations in main function (though we can do that in mapper).
		 * Therefore, we use the AWS API to do this.
		 */
		Integer singleSource = Integer.parseInt(args[2]);

		/*
		 * This part of code can transform the format of input locally
		 * 
		 * AWSCredentials credentials = new
		 * BasicAWSCredentials("AKIAIZ5IA2UP27LMYL6A",
		 * "PSclr7E9LujwpFd8/g5ILusfy+9dqUsuHTTbgmzW");
		 * 
		 * //We firstly transform the file format AmazonS3Client s3Client = new
		 * AmazonS3Client(credentials);
		 * 
		 * S3Object s3object = s3Client.getObject(new
		 * GetObjectRequest("comp9313.z5055605",String.format("problem2/%s",
		 * args[0])));
		 * 
		 * 
		 * FileSystem hdfs = FileSystem.get(new Configuration());
		 * 
		 * //Path inPath = new Path(IN);
		 * 
		 * //FSDataInputStream inputStream = hdfs.open(inPath);
		 * 
		 * BufferedReader inputBuffer = new BufferedReader(new
		 * InputStreamReader(s3object.getObjectContent()));
		 * 
		 * Path newinPath = new Path("/comp9313/input/" + IN + "_1");
		 * 
		 * FSDataOutputStream newFormatInput = hdfs.create(newinPath);
		 * 
		 * String lineContent = "";
		 * 
		 * Integer curSourceNode = 0;
		 * 
		 * Integer singleSource = Integer.parseInt(args[2]);
		 * 
		 * String tmpNewFmtRecord = curSourceNode.toString() + " " + "0.0" +
		 * "|";
		 * 
		 * while ((lineContent = inputBuffer.readLine()) != null) { //filtering
		 * out the first several lines which starts with # if
		 * (!lineContent.startsWith("#")){
		 * 
		 * String[] edgeRecord = lineContent.split("\\s+");
		 * 
		 * 
		 * if (Integer.parseInt(edgeRecord[1]) != curSourceNode) {
		 * 
		 * //Actually, Y or N is not used in the program, but it can be used to
		 * see if the distance has been updated in last run tmpNewFmtRecord +=
		 * "|Y\n";
		 * 
		 * newFormatInput.writeBytes(tmpNewFmtRecord);
		 * 
		 * curSourceNode = Integer.parseInt(edgeRecord[1]); //the shortest
		 * distance for the source node is 0.0 if
		 * (Integer.parseInt(edgeRecord[1]) == singleSource){
		 * 
		 * tmpNewFmtRecord = edgeRecord[1] + " " + "0.0" + "|" + edgeRecord[2] +
		 * ":" + edgeRecord[3].toString() + ","; }
		 * 
		 * else{
		 * 
		 * tmpNewFmtRecord = edgeRecord[1] + " " + maxDistance.toString() + "|"
		 * + edgeRecord[2] + ":" + edgeRecord[3].toString() + ","; }
		 * 
		 * }
		 * 
		 * else {
		 * 
		 * tmpNewFmtRecord += edgeRecord[2] + ":" + edgeRecord[3] + ",";
		 * 
		 * } }
		 * 
		 * 
		 * } // to fix the problem of last line. tmpNewFmtRecord += "|Y";
		 * 
		 * newFormatInput.writeBytes(tmpNewFmtRecord);
		 * 
		 * newFormatInput.close();
		 * 
		 */

		/*
		 * The following part of code transform the code for input by mapreduce.
		 */

		Path newinPath = new Path("/comp9313/input/newinput");

		Configuration confFormat = new Configuration();

		confFormat.set("queryNode", singleSource.toString());

		Job jobFormat = Job.getInstance(confFormat, "InputFormatTransformation");

		jobFormat.setJarByClass(SingleSourceSP.class);

		jobFormat.setMapperClass(FormatTransMapper.class);

		jobFormat.setReducerClass(FormatTransReducer.class);

		jobFormat.setOutputKeyClass(Text.class);

		jobFormat.setOutputValueClass(Text.class);

		jobFormat.setNumReduceTasks(1);

		FileInputFormat.addInputPath(jobFormat, new Path(IN));

		FileOutputFormat.setOutputPath(jobFormat, newinPath);

		jobFormat.waitForCompletion(true);

		// start to configure mapreduce

		String input = "/comp9313/input/newinput" + "/part-r-00000";
		

		System.out.printf("The input is %s", input);

		String output = OUT;
		// this path is for the intermediate results.
		String intermediateOutputBase = "/comp9313/output";

		String intermediateOutput = intermediateOutputBase;

		int runCycle = 0;

		boolean isdone = false;

		while (isdone == false) {

			System.out.printf("This is the %d round of run for mapreduce job\n", ++runCycle);

			Configuration conf = new Configuration();

			conf.set("JobSeq", "NotLast");

			conf.set("queryNode", singleSource.toString());

			Job job = Job.getInstance(conf, "GraphEdgeReverse");

			job.setJarByClass(SingleSourceSP.class);

			job.setMapperClass(SPMapper.class);

			job.setReducerClass(SPReducer.class);

			job.setOutputKeyClass(IntWritable.class);

			job.setOutputValueClass(Text.class);

			job.setNumReduceTasks(38);

			FileInputFormat.addInputPath(job, new Path(input));

			FileOutputFormat.setOutputPath(job, new Path(intermediateOutput));

			job.waitForCompletion(true);

			Counters counters = job.getCounters();

			Counter updateCounter = counters.findCounter(UpdateCounter.UpdateDistance);

			System.out.printf("updateCounter value is %d\n", updateCounter.getValue());

			if (updateCounter.getValue() == 0.0) {

				isdone = true;
				// if isdone has been set to true, we run one more time to
				// output the result to Amazaon S3.
				Configuration confFinal = new Configuration();

				confFinal.set("JobSeq", "Last");

				confFinal.set("queryNode", singleSource.toString());

				Job jobFinal = Job.getInstance(confFinal, "GraphEdgeReverse");

				jobFinal.setJarByClass(SingleSourceSP.class);

				jobFinal.setMapperClass(SPMapper.class);

				jobFinal.setReducerClass(SPReducer.class);

				jobFinal.setOutputKeyClass(IntWritable.class);

				jobFinal.setOutputValueClass(Text.class);

				jobFinal.setNumReduceTasks(1);

				FileInputFormat.addInputPath(jobFinal, new Path(input));

				FileOutputFormat.setOutputPath(jobFinal, new Path(output));

				jobFinal.waitForCompletion(true);
			}

			// input = intermediateOutput + "/part-r-00000";

			input = intermediateOutput;

			System.out.println(input);

			intermediateOutput = intermediateOutputBase + System.nanoTime();

		}

	}

}
