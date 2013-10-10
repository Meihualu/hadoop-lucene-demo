package org.codemomentum;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Illustrates how to implement a indexer as hadoop map reduce job.
 */
public class EsIndexerJob {

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            String usage = "IndexerJob <in text file/dir> <out katta index dir> <numOfShards>";
            System.out.println(usage);
            System.exit(1);
        }

        EsIndexerJob indexerJob = new EsIndexerJob();
        String input = args[0];
        String output = args[1];
        int numOfShards = Integer.parseInt(args[2]);
        indexerJob.startIndexer(input, output, numOfShards);

    }

    public void startIndexer(String path, String finalDestination, int numOfShards) throws IOException {
        // create job conf with class pointing into job jar.
        JobConf jobConf = new JobConf(EsIndexerJob.class);
        jobConf.setJobName("indexer");
        jobConf.setMapRunnerClass(Indexer.class);
        // alternative use a text file and a TextInputFormat
        jobConf.setInputFormat(TextInputFormat.class);

        Path input = new Path(path);
        FileInputFormat.setInputPaths(jobConf, input);
        // we just set the output path to make hadoop happy.
        FileOutputFormat.setOutputPath(jobConf, new Path(finalDestination));
        // setting the folder where lucene indexes will be copied when finished.
        jobConf.set("finalDestination", finalDestination);
        // important to switch spec exec off.
        // We dont want to have something duplicated.
        jobConf.setSpeculativeExecution(false);

        // The num of map tasks is equal to the num of input splits.
        // The num of input splits by default is equal to the num of hdf blocks
        // for the input file(s). To get the right num of shards we need to
        // calculate the best input split size.

        FileSystem fs = FileSystem.get(input.toUri(), jobConf);
        FileStatus[] status = fs.globStatus(input);
        long size = 0;
        for (FileStatus fileStatus : status) {
            size += fileStatus.getLen();
        }
        long optimalSplisize = size / numOfShards;
        jobConf.set("mapred.min.split.size", "" + optimalSplisize);

        // give more mem to lucene tasks.
        jobConf.set("mapred.child.java.opts", "-Xmx2G");
        jobConf.setNumMapTasks(1);
        jobConf.setNumReduceTasks(0);
        JobClient.runJob(jobConf);
    }

    public static class Indexer implements MapRunnable<LongWritable, Text, Text, Text> {

        Node node;

        Client client;


        BulkRequestBuilder currentRequest;

        private JobConf _conf;

        private AtomicLong totalBulkItems = new AtomicLong();

        private int bulkSize = 50000;

        String indexName;

        public void configure(JobConf conf) {
            _conf = conf;
            node = nodeBuilder().clusterName("mac").node();
            client = node.client();
            indexName = "" + System.currentTimeMillis() + "-" + new Random().nextInt();
            initialize_index(indexName);
            currentRequest = client.prepareBulk();
        }

        @SuppressWarnings("deprecation")
        public void run(RecordReader<LongWritable, Text> reader, OutputCollector<Text, Text> output, final Reporter report)
                throws IOException {
            LongWritable key = reader.createKey();
            Text value = reader.createValue();

            String tmp = _conf.get("hadoop.tmp.dir");
            long millis = System.currentTimeMillis();
            String shardName = "" + millis + "-" + new Random().nextInt();
            File file = new File(tmp, shardName);
            report.progress();


            report.setStatus("Adding documents...");
            while (reader.next(key, value)) {
                report.progress();

                Map<String, Object> map = new HashMap<String, Object>();
                map.put("content", value.toString());

                currentRequest.add(Requests.indexRequest(indexName).type("testing").source(map));

                processBulkIfNeeded();
            }

            report.setStatus("Done adding documents.");
            report.setStatus("Closing index...");
            close();
            report.setStatus("Closing done!");
        }

        private void initialize_index(String indexName) {
            try {
                client.admin().indices().prepareCreate(indexName).execute().actionGet();
            } catch (Exception e) {
            }
        }

        public void close() throws IOException {
            if (currentRequest.numberOfActions() > 0) {
                try {
                    BulkResponse response = currentRequest.execute().actionGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            client.close();
            if (node != null) {
                node.close();
            }
        }

        private void processBulkIfNeeded() {
            totalBulkItems.incrementAndGet();
            if (currentRequest.numberOfActions() >= bulkSize) {
                currentRequest.execute().actionGet();
                currentRequest = client.prepareBulk();
            }
        }

    }


}
