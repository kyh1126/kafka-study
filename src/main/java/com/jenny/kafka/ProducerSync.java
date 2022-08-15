package com.jenny.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerSync {
    public static void main(String[] args) {
        // 1. Properties 객체 생성
        Properties props = new Properties();
        // 2. 브로커 리스트 정의
        props.put("bootstrap.servers", "kafka1:9091,kafka2:9092,kafka3:9093");
        // 3. 메시지 키와 밸류는 문자열 타입이므로 카프카의 기본 StringSerializer를 지정
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 4. Properties 객체를 전달해 새 프로듀서 생성
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 3; i++) {
                // 5. ProducerRecord 객체 생성
                ProducerRecord<String, String> record = new ProducerRecord<>("peter-overview01", "Apache Kafka is a " +
                        "distributed streaming platform - " + i);
                // 6. get() 메소드를 이용해 카프카의 응답을 기다림
                // 메시지가 성공적으로 전송되지 않으면 예외가 발생하고, 에러가 없다면 RecordMetadata를 얻음
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Received Message: %s\n", metadata.topic(), metadata.partition()
                        , metadata.offset(), record.key(), record.value());
            }
        } catch (Exception e) {
            // 7. 카프카로 메시지를 보내기 전과 보내는 동안 에러가 발생하면 예외가 발생함
            e.printStackTrace();
        } finally {
            // 8. 프로듀서 종료
            producer.close();
        }
    }
}
