package se.phaseshift.hadoop.climate.stations;

import java.util.Iterator;

import com.github.fge.jsonschema.report.ProcessingReport;
import com.github.fge.jsonschema.report.ProcessingMessage;

public class JsonValidationException extends Exception implements Iterable<ProcessingMessage> {
    private ProcessingReport processingReport;

    public JsonValidationException(ProcessingReport processingReport) {
	super("JSON Validation Exception");
	this.processingReport = processingReport;
    }

    public Iterator<ProcessingMessage> iterator() {        
        Iterator<ProcessingMessage> ipm = this.processingReport.iterator();
        return ipm; 
    }
}

