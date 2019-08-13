package com.shapira.examples.kstreamavg;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

public class TechoDeserializationExceptionHandler implements DeserializationExceptionHandler {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}
	

	@Override
	public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record,
			Exception exception) {
		// TODO Auto-generated method stub
		System.out.println("Exception While handling serialization:"+new String(record.value()).toString());
		return DeserializationHandlerResponse.CONTINUE;
	}

}
