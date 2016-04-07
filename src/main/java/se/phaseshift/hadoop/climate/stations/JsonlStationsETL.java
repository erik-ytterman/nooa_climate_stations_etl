package se.phaseshift.hadoop.climate.stations;

import java.lang.StringBuilder;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.nio.charset.StandardCharsets;

import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.avro.Schema;

import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class JsonlStationsETL extends Configured implements Tool {

    public static void main(String[] args)  throws Exception {
	if(args.length >= 3) {
	    int res = ToolRunner.run(new Configuration(), new JsonlStationsETL(), args);
	    System.exit(res);
	} else {
	    System.out.println("ERROR");
	    System.exit(0);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	// Get paths
 	Path inputPath = new Path(args[0]);
	Path outputPath = new Path(args[1]);				  
	Path outputSchemaPath = new Path(args[2]);
	Path inputSchemaPath = new Path(args[3]);

	// Create configuration and filesystem objects
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);

	// Clean output area, othetwise job will terminate
	fs.delete(outputPath, true);

	// Read the output (AVRO) schema
        String outputSchemaString = inputStreamToString(fs.open(outputSchemaPath));

	// Add output schema string to configuration for the mappers of the job
	conf.set("climate.stations.output.schema", outputSchemaString);

	// Read the input (JSON) schema
	String inputSchemaString = inputStreamToString(fs.open(inputSchemaPath));

	// Add input schema string to configuration for the mappers of the job
	conf.set("climate.stations.input.schema", inputSchemaString);

	// Create job
	Job job = Job.getInstance(conf);
	job.setJarByClass(JsonlStationsETL.class);
	job.setJobName("Ghcnd_Stations_Jsonl_ETL");

	TextInputFormat.addInputPath(job, inputPath);

	job.setInputFormatClass(TextInputFormat.class);
	job.setMapperClass(JsonlStationsETLMapper.class);
	job.setPartitionerClass(HashPartitioner.class);

	Schema outputSchema = new Schema.Parser().parse(outputSchemaString);

        AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, outputSchema);
        AvroParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
        AvroParquetOutputFormat.setCompressOutput(job, true);
        AvroParquetOutputFormat.setBlockSize(job, 500 * 1024 * 1024);	
	job.setOutputFormatClass(AvroParquetOutputFormat.class);

	job.setNumReduceTasks(0);
	job.setReducerClass(Reducer.class);

	return job.waitForCompletion(true) ? 0 : 1;
    }

    private String inputStreamToString(InputStream is) throws IOException {
	BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));     
	StringBuilder buffer = new StringBuilder(8192);
	String str = null;

	while ((str = reader.readLine()) != null) { buffer.append(str); }

	return buffer.toString();
    }
}