package com.shapira.examples.kstreamavg;

import java.util.Properties;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import io.confluent.shaded.com.google.gson.Gson;

//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
//import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;

//import io.confluent.examples.streams.GenericAvroSerde;
///import io.confluent.examples.streams.PageViewRegionLambdaExample;


public class StreamingAvg2 {
    static Logger log = Logger.getLogger(StreamingAvg2.class.getName());

    @SuppressWarnings("deprecation")
	public static void main(final String[] args) throws Exception {

      //  final String bootstrapServers = args.length > 1 ? args[0] : "localhost:9092";
    	final String bootstrapServers ="localhost:9092";
        //final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
        
    	
        //Serde<SensorData> sensorDataSerde = Serdes.serdeFrom(new SensorDataSerializer(), new SensorDataDeSerializer());
        SensorDataSerde sensorDataSerde = new SensorDataSerde();
        
        
        int windowTime=Integer.valueOf(args[0]).intValue();
        
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "averagesensordata34");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "averagesensordata34-client");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Where to find the Confluent schema registry instance(s)
  //      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Specify default (de)serializers for record keys and for record values.
       // streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, sensorDataSerde.getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, sensorDataSerde.serializer().getClass());
      //  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, sensorDataSerde.deserializer().getClass());
    //    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, sensorDataSerde.getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, sensorDataSerde.getClass().getName());
        //streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Double().getClass().getName());
    //    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
       // streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,TechoDeserializationExceptionHandler.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, Serdes.Double().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();

        // Create a stream of page view events from the PageViews topic, where the key of
        // a record is assumed to be null and the value an Avro GenericRecord
        // that represents the full details of the page view event. See `pageview.avsc` under
        // `src/main/avro/` for the corresponding Avro schema.
        final KStream<String, SensorData> views = builder.stream("mt_test");

       
        // Create a keyed stream of page view events from the PageViews stream,
        // by extracting the user id (String) from the Avro value
        
      
        
        /*final KStream<String, String> viewsByUser = views.map((dummy, record) ->
            new KeyValue<>(record.get("n").toString(), record));
        viewsByUser.foreach((key,val) ->{
        	System.out.println("Key is:"+key);
        	System.out.println("Value is is:"+val);
        });*/
        /*final KStream<String, SensorData> viewsByUser = views.map((key, value) ->
        new KeyValue<>(key.getN(), value));*/
        Gson gson = new Gson();
        KStream<String, SensorData> dataByKey = views.map((key, valuejson) ->{
        	
        	System.out.println("keyJson is:"+key);
        	System.out.println("valueJson is:"+valuejson);
        	 
        	//SensorData value=gson.fromJson(valuejson, SensorData.class);
        	
        	return new KeyValue<>(valuejson.getN(), valuejson);
        	/*if(key!=null)
        		return new KeyValue<>(key.getN(), valuejson);
        	else
        		return new KeyValue<>("Default", valuejson);*/
        });
   
        dataByKey.foreach((key,val) ->{
	    	System.out.println("Key is:"+key);
	    	System.out.println("Value is is:"+val);
        });
        //TimeWindowedKStream< String, Double> timebuckets=dataByKey.groupByKey().windowedBy(TimeWindows.of(windowTime*1000));
      //  TimeWindowedKStream< String, SensorData> timebuckets=views.groupByKey().windowedBy(TimeWindows.of(windowTime*1000));
        TimeWindowedKStream< String, SensorData> timebuckets=dataByKey.groupByKey().windowedBy(TimeWindows.of(windowTime*1000));
        KTable<Windowed<String>, Long> keyCounts=timebuckets.count();
        //keyCounts.toStream().foreach((key,val)->System.out.println("Count is:"+val));
      //  Initializer<Double> initializer=new StreamInitializer();
        KTable<Windowed<String>, SensorData> aggregateValues=timebuckets.reduce((SensorData v1,SensorData v2)->{
        	v1.setValue(v1.getValue()+v2.getValue());
        return v1;
        });
        //KTable<Windowed<String>, Double> aggregateValues=dataByKey.groupByKey().windowedBy(TimeWindows.of(windowTime*1000)).aggregate(initializer, aggregator);
      //    KTable<Windowed<String>, Double> aggregateValues=timebuckets.aggregate(() ->0.0, (key,value,aggregartevale)-> aggregartevale+value.getValue());
     //   KTable<Windowed<String>, Double> aggregateValues=timebuckets.aggregate(() ->0.0, (key,value,aggregartevale)-> aggregartevale+value.getValue());
       // KTable<Windowed<String>, Long> keyCounts=timebuckets.count();
        KTable<Windowed<String>, Double> averages=keyCounts.join(aggregateValues, (countvals,aggvales)->aggvales.getValue()/countvals);
        
        
        averages.toStream().foreach((key,val)->{
        	System.out.println("Key is:"+key);
        	System.out.println("Average is:"+val);
        });
       // dataByKey.groupByKey().reduce()
        
        /*
        // Create a changelog stream for user profiles from the UserProfiles topic,
        // where the key of a record is assumed to be the user id (String) and its value
        // an Avro GenericRecord.  See `userprofile.avsc` under `src/main/avro/` for the
        // corresponding Avro schema.
        final KTable<String, GenericRecord> userProfiles = builder.table("UserProfiles");

        // Create a changelog stream as a projection of the value to the region attribute only
        final KTable<String, String> userRegions = userProfiles.mapValues(record ->
          record.get("region").toString());

        // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
        // In this example, we want to create an intermediate GenericRecord to hold the view region.
        // See `pageviewregion.avsc` under `src/main/avro/`.
        final InputStream
          pageViewRegionSchema =
          PageViewRegionLambdaExample.class.getClassLoader()
            .getResourceAsStream("avro/io/confluent/examples/streams/pageviewregion.avsc");
        final Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

        final KTable<Windowed<String>, Long> viewsByRegion = viewsByUser
          .leftJoin(userRegions, (view, region) -> {
            final GenericRecord viewRegion = new GenericData.Record(schema);
            viewRegion.put("user", view.get("user"));
            viewRegion.put("page", view.get("page"));
            viewRegion.put("region", region);
            return viewRegion;
          })
          .map((user, viewRegion) -> new KeyValue<>(viewRegion.get("region").toString(), viewRegion))
          // count views by region, using hopping windows of size 5 minutes that advance every 1 minute
          .groupByKey() // no need to specify explicit serdes because the resulting key and value types match our default serde settings
          .windowedBy(TimeWindows.of(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(1)))
          .count();

        // Note: The following operations would NOT be needed for the actual pageview-by-region
        // computation, which would normally stop at `count` above.  We use the operations
        // below only to "massage" the output data so it is easier to inspect on the console via
        // kafka-console-consumer.
        final KStream<String, Long> viewsByRegionForConsole = viewsByRegion
          // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
          // and by also converting the record key from type `Windowed<String>` (which
          // kafka-console-consumer can't print to console out-of-the-box) to `String`
          .toStream((windowedRegion, count) -> windowedRegion.toString());

        viewsByRegionForConsole.to("PageViewsByRegion", Produced.with(stringSerde, longSerde));
*/
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // Always (and unconditionally) clean local state prior to starting the processing topology.
        // We opt for this unconditional call here because this will make it easier for you to play around with the example
        // when resetting the application for doing a re-run (via the Application Reset Tool,
        // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
        //
        // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
        // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
        // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
        // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
        // See `ApplicationResetExample.java` for a production-like example.
        
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
      
    }
}
