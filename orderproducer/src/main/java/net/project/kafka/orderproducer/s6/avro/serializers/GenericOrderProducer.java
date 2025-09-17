package net.project.kafka.orderproducer.s6.avro.serializers;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.project.kafka.orderproducer.s4.producerconsumer.OrderCallBack;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class GenericOrderProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "http://172.25.50.202:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url", "http://172.25.50.202:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Parser parser = new Schema.Parser();
        // worked fine for 'net.project.kafka.orderproducer.s5.serializerdeserializer.model' and 'net.project.kafka.s6.avro' namespaces
        Schema schema = parser.parse("{\n"
                + "\"namespace\": \"net.project.kafka.s6.avro\",\n"
                + "\"type\": \"record\",\n"
                + "\"name\": \"Order\",\n"
                + "\"fields\": [\n"
                + "{\"name\": \"customerName\",\"type\":\"string\"},\n"
                + "{\"name\": \"product\",\"type\":\"string\"},\n"
                + "{\"name\": \"quantity\",\"type\":\"int\"}\n"
                + "]\n"
                + "}");

        GenericRecord order = new GenericData.Record(schema);
        order.put("customerName", "Mohan");
        order.put("product", "Avro Mac Book Pro");
        order.put("quantity", 100);

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderAvroGRTopic", order.get("customerName").toString(), order);
        try {
            producer.send(record, new OrderCallBack());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}

/*
Confluent Commands & URLs:
 - confluent local services start
 - http://172.25.50.202:9021/clusters
 - http://172.25.50.202:8081/schemas
 - confluent local services stop
*/

/*
[
  {
    "subject": "OrderAvroGRTopic-key",
    "version": 1,
    "id": 1,
    "schema": "\"string\""
  },
  {
    "subject": "OrderAvroGRTopic-value",
    "version": 1,
    "id": 3,
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"net.project.kafka.s6.avro\",\"fields\":[{\"name\":\"customerName\",\"type\":\"string\"},{\"name\":\"product\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"}]}"
  },
  {
    "subject": "OrderAvroGRTopic-value",
    "version": 2,
    "id": 4,
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"net.project.kafka.orderproducer.s5.serializerdeserializer.model\",\"fields\":[{\"name\":\"customerName\",\"type\":\"string\"},{\"name\":\"product\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"}]}"
  },
  {
    "subject": "OrderAvroTopic-key",
    "version": 1,
    "id": 1,
    "schema": "\"string\""
  },
  {
    "subject": "OrderAvroTopic-value",
    "version": 1,
    "id": 2,
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"net.project.kafka.orderproducer.s6.avro\",\"fields\":[{\"name\":\"customerName\",\"type\":\"string\"},{\"name\":\"product\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"}]}"
  },
  {
    "subject": "OrderAvroTopic-value",
    "version": 2,
    "id": 3,
    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"net.project.kafka.s6.avro\",\"fields\":[{\"name\":\"customerName\",\"type\":\"string\"},{\"name\":\"product\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"}]}"
  }
]
*/
