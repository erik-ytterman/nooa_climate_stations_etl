package se.phaseshift.hadoop.climate.stations;

import java.lang.InterruptedException;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonReader;
import javax.json.JsonObject;

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
	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	JsonReader jsonReader = Json.createReader(new StringReader(value.toString()));
	JsonObject jsonObject = jsonReader.readObject();

	// Extract data from JSON line instance
	String stationId        = jsonObject.getString("id");
	Float  stationLatitude  = new Float(jsonObject.getJsonNumber("latitude").doubleValue());
	Float  stationLongitude = new Float(jsonObject.getJsonNumber("longitude").doubleValue());
	Float  stationElevation = new Float(jsonObject.getJsonNumber("elevation").doubleValue());
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
