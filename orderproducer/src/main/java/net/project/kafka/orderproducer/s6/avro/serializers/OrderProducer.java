package net.project.kafka.orderproducer.s6.avro.serializers;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.project.kafka.orderproducer.s4.producerconsumer.OrderCallBack;
import net.project.kafka.s6.avro.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class OrderProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "http://192.168.1.112:9092");
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        // confluent local services start
        // http://192.168.1.112:8081/schemas
        props.setProperty("schema.registry.url", "http://192.168.1.112:8081");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Mohan","avro iPhone14",3);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString(), order);

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
Steps to set up Confluent: (https://www.youtube.com/watch?v=D5TSOt3hVTU)
 - Install Lubuntu in Oracle Virtual Box
 - Create folder 'confluent' and download Confluent 6.2.0 (https://packages.confluent.io/archive/6.2/confluent-6.2.0.tar.gz) to this folder. Then extract
 - Set up below environment variables:
        cd /home/mohan/confluent
        export CONFLUENT_HOME=/home/mohan/confluent/confluent-6.2.0
        printenv CONFLUENT_HOME
        export PATH=$CONFLUENT_HOME/bin:$PATH
        printenv PATH
        confluent local services start
 - ifconfig -> gives ip address of Lubuntu OS (192.168.1.112)
 - Access this URL from host OS
 - http://192.168.1.112:8081/schemas
 - http://192.168.1.112:9021/clusters
*/

/*
pom.xml:
		<dependency>
			<groupId>org.apache.avro</groupId>
			<artifactId>avro</artifactId>
			<version>1.12.0</version>
		</dependency>
		<dependency>
			<groupId>io.confluent</groupId>
			<artifactId>kafka-avro-serializer</artifactId>
			<version>6.2.15</version>
		</dependency>

		<plugin>
			<groupId>org.apache.avro</groupId>
			<artifactId>avro-maven-plugin</artifactId>
			<version>1.10.2</version>
			<executions>
				<execution>
					<phase>generate-sources</phase>
					<goals>
						<goal>schema</goal>
					</goals>
					<configuration>
						<sourceDirectory>${project.basedir}/src/main/resources/</sourceDirectory>
						<outputDirectory>${project.basedir}/src/main/java</outputDirectory>
					</configuration>
				</execution>
			</executions>
		</plugin>

		<repositories>
			<repository>
				<id>confluent</id>
				<url>https://packages.confluent.io/maven/</url>
			</repository>
		</repositories>
*/

/*
http://192.168.1.112:8081/schemas
 - Auto generated schema
 - For first time, Order class was generated in net.project.kafka.orderproducer.s6.avro package (version 1)
 - Later I changed to net.project.kafka.s6.avro so that Producer and Consumer can look for generated class file in common package.
 - Then Kafka auto-generated another schema (version 2) as below
[
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
