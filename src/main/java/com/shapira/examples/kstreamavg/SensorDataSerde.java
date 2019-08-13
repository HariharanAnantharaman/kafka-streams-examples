package com.shapira.examples.kstreamavg;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

public class SensorDataSerde extends WrapperSerde<SensorData> {
	
	public SensorDataSerde() {
        super(new SensorDataSerializer(), new SensorDataDeSerializer());
    }


}
