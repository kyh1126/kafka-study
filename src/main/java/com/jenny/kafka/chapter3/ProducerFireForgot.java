package com.jenny.kafka.chapter3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ProducerFireForgot {
    public static void main(String[] args) {

        // 1. Properties 객체 생성
        Properties props = new Properties();
        // 2. 브로커 리스트 정의
        props.put("bootstrap.servers", "0.0.0.0:9091,0.0.0.0:9092,0.0.0.0:9093");
//        props.put("bootstrap.servers","peter-kafka01.foo.bar:9092,peter-kafka02.foo.bar:9092,peter-kafka03.foo
//        .bar:9092");

        // 3. 메시지 키와 밸류는 문자열 타입이므로 카프카의 기본 StringSerializer를 지정
        // Record의 key와 value는 문자이기 때문에 전송시 byte로 변환해야한다.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 4. Properties 객체를 전달해 새 프로듀서 생성
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < 3; i++) {
                // 5. ProducerRecord 객체 생성
                ProducerRecord<String, String> record = new ProducerRecord<>("peter-overview01",//"peter-basic01",
                        "Apache kafka is a distributed streaming platform - " + i);
                // 6. send() 메소드를 사용해 메시지를 전송한 후 자바 Future 객체로 RecordMetadata를 리턴받지만,
                // 리턴값을 무시하므로 메시지가 성공적으로 전송됐는지 알 수 없음
                producer.send(record);
            }
        } catch (Exception e) {
            // 7. 카프카 브로커에게 메시지를 전송한 후의 에러는 무시하지만, 전송 전에 에러가 발생하면 예외를 처리할 수 있음
            e.printStackTrace();
        } finally {
            // 8. 프로듀서 종료
            producer.close();
        }
    }
}
