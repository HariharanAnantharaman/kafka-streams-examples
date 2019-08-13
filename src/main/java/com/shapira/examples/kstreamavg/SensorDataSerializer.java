package com.shapira.examples.kstreamavg;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class SensorDataSerializer implements Serializer<SensorData> {

	//private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		Serializer.super.configure(configs, isKey);
	}

	@Override
	public byte[] serialize(String topic, Headers headers, SensorData data) {
		// TODO Auto-generated method stub
		return serialize(topic, data);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		Serializer.super.close();
	}

	@Override
	public byte[] serialize(String topic,SensorData arg1) {
		// TODO Auto-generated method stub
		if (Objects.isNull(arg1)) {
		      return null;
		    }
		    try {
		      return arg1.toString().getBytes();
		    } catch (Exception e) {
		      System.out.println("Exception happened while serializing");
		      e.printStackTrace();
		      return null;
		    }
	}
}


