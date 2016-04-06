package se.phaseshift.hadoop.climate.stations;

import java.lang.InterruptedException;

import java.io.IOException;
import java.io.StringReader;

import org.json.JSONObject;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericRecord;

import org.apache.parquet.Log;

public class JsonlStationsETLMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
    private GenericRecordBuilder recordBuilder = null;

    @Override
    public void setup(Context context) {
	Schema stationSchema;
	Configuration conf = context.getConfiguration();
	
	// Create a record builder for output (AVRO) records
	stationSchema = new Schema.Parser().parse(conf.get("climate.stations.schema"));
	this.recordBuilder = new GenericRecordBuilder(stationSchema);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	FileSplit fileSplit   = (FileSplit) context.getInputSplit();
	JSONObject jsonObject = new JSONObject(value.toString());

	// Extract data from JSON line instance
	String stationId        = jsonObject.getString("id");
	Float  stationLatitude  = new Float(jsonObject.getDouble("latitude"));
	Float  stationLongitude = new Float(jsonObject.getDouble("longitude"));
	Float  stationElevation = new Float(jsonObject.getDouble("elevation"));
	String stationName      = jsonObject.getString("name");
	String fileName         = fileSplit.getPath().getName();

	// Configre generic AVRO record output data
	this.recordBuilder.set("id", stationId);
	this.recordBuilder.set("latitude" , stationLatitude);
	this.recordBuilder.set("longitude", stationLongitude);
	this.recordBuilder.set("elevation", stationElevation);
	this.recordBuilder.set("name", stationName);

	// Generate AVRO record
	GenericRecord record = this.recordBuilder.build();

	// Dispatch data
	context.write(null, record);
    }

    @Override
    public void cleanup(Context context) {
    }    
}
