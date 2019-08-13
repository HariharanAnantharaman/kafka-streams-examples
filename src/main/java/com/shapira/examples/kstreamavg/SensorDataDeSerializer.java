package com.shapira.examples.kstreamavg;

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.shaded.com.google.gson.Gson;
import io.confluent.shaded.com.google.gson.JsonSyntaxException;

public class SensorDataDeSerializer  implements Deserializer<SensorData> {

	

	

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		Deserializer.super.configure(configs, isKey);
	}

	@Override
	public SensorData deserialize(String topic, Headers headers, byte[] data) {
		// TODO Auto-generated method stub
		StringDeserializer deserializer=new StringDeserializer();
		String val=deserializer.deserialize(topic, headers,data);
		Gson gson=new Gson();
		deserializer.close();
		return gson.fromJson(val, SensorData.class);
		
	}
	

	@Override
	public void close() {
		// TODO Auto-generated method stub
		Deserializer.super.close();
	}

	@Override
	public SensorData deserialize( String topic,  byte[] arg1) {
		// TODO Auto-generated method stub
		try {
			StringDeserializer deserializer=new StringDeserializer();
			String val=deserializer.deserialize(topic, arg1);
			//String string=new String(arg1);
			deserializer.close();
			Gson gson=new Gson();
			return gson.fromJson(val, SensorData.class);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Exception happened while deserializing");
			e.printStackTrace();
			return null;
		}
		//return null;
	}

}
