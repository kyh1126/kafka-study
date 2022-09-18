package com.jenny.kafka.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerAsync {
    public static void main(String[] args) {
        // 1. Properties 객체 생성
        Properties props = new Properties();
        // 2. 브로커 리스트 정의
        props.put("bootstrap.servers", "0.0.0.0:9091,0.0.0.0:9092,0.0.0.0:9093");
        // 3. 메시지 키와 밸류는 문자열 타입이므로 카프카의 기본 StringSerializer를 지정
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 4. Properties 객체를 전달해 새 프로듀서 생성
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 3; i++) {
                // 5. ProducerRecord 객체 생성
                ProducerRecord<String, String> record = new ProducerRecord<>("peter-overview01", "Apache Kafka is a distributed streaming platform - " + i); //ProducerRecord 오브젝트를 생성합니다.
                // 6. 프로듀서에서 레코드를 보낼 때 콜백 오브젝트를 같이 보냄
                producer.send(record, new PeterProducerCallback(record));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 7. 프로듀서 종료
            producer.close();
        }
    }
}
