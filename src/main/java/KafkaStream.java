import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.HashMap;
import java.util.Properties;




public class KafkaStream {
    public static void main(String[] args){
        String topic = "access-log";
        String bootstrapServers = "10.0.3.4:9092";

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "Apache-HTTP-CODE-FREQ"
        );
        streamsConfiguration.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        final Serde<String> stringSerde = Serdes.String();
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> source = builder.stream(stringSerde, stringSerde, topic);

        // Create Hashmaps that will be utilized
        HashMap<String, Integer> ipErrorMap = new HashMap<>();
        HashMap<String, Integer> ipRequestMap = new HashMap<>();

        // Create Producer
        AnomalyProducer producer = new AnomalyProducer("anomalies");


        source.foreach(new ForeachAction<String, String>() {
            public void apply(String key, String value) {
                // Parse Logs
                String[] parsedLogs = logParser.parseAccessLogs(value);

                ipErrorMap.putIfAbsent(parsedLogs[0], 0);
                ipRequestMap.putIfAbsent(parsedLogs[0], 0);

                // Update total Requests
                updateMap(parsedLogs, ipRequestMap);

                // Check if error return
                if(parsedLogs[4].equals("404")) {
                    System.out.println("Error Detected");
                    // Update Hash Table for Error Call
                    updateMap(parsedLogs, ipErrorMap);
                }

                // Get the Total Requests & Total Error Requests
                double totalReqs = sumMap(ipRequestMap);
                double totalErrs = sumMap(ipErrorMap);

                // Calculate thresholds for errors = avg(error reqs / total reqs) across ips
                double errorThreshold = totalErrs / totalReqs / ipRequestMap.size();

                // Calculate average for request = avg(reqs) across ips
                double requestAvg = totalReqs / ipErrorMap.size();

                // Calculate Stream Ip current Thresholds
                double errors = ipErrorMap.get(parsedLogs[0]);
                double reqs = ipRequestMap.get(parsedLogs[0]);
                double currentErrThreshold = errors / reqs;


                // Check if threshold reached
                if(currentErrThreshold > errorThreshold ||
                        (reqs >= requestAvg * 1.5 || reqs <= requestAvg / 1.5)){
                    System.out.println("Threshold for Errors Reached for ip: " + parsedLogs[0]);
                    producer.send(parsedLogs[0], "Irregular Traffic - Anomaly - " + parsedLogs[0]);
                }
            }
        });

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

    }

    public static void updateMap(String[] parsedLogs, HashMap<String, Integer> ipRequestMap) {
        Integer count = ipRequestMap.get(parsedLogs[0]);
        //System.out.println("Count = " + count);
        if (count == null) {
            ipRequestMap.put(parsedLogs[0], 1);
        } else {
            count++;
            ipRequestMap.put(parsedLogs[0], count);
        }
    }

    public static double sumMap(HashMap<String, Integer> sumMap){
        double count = 0;
        for(Integer n: sumMap.values()){
            count += n;
        }
        return count;
    }
}
