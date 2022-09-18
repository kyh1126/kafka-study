package com.jenny.kafka.chapter3;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerSync {
    public static void main(String[] args) {
        // 1. Properties 객체 생성
        Properties props = new Properties();
        // 2. 브로커 리스트 정의
        props.put("bootstrap.servers", "0.0.0.0:9091,0.0.0.0:9092,0.0.0.0:9093");
        // 3. 컨슈머 그룹 아이디 정의
        props.put("group.id", "peter-consumer01");
        // 4. 오토 커밋을 사용하지 않음
        props.put("enable.auto.commit", "false");
        // 5. 컨슈머 오프셋을 찾지 못하는 경우 latest로 초기화하며 가장 최근부터 메시지를 가져옴
        props.put("auto.offset.reset", "latest");
        // 6. 문자열을 사용했으므로 StringSerializer를 지정
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 7. Properties 객체를 전달해 새 컨슈머 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        // 8. 구독할 토픽을 지정
        consumer.subscribe(Arrays.asList("peter-overview01"));

        try {
            // 9. 무한 루프 시작. 메시지를 가져오기 위해 카프카에 지속적으로 poll()을 함
            while (true) {
                // 10. 컨슈머는 폴링하는 것을 계속 유지하며, 타임아웃 주기를 설정. 해당 시간만큼 블록함
                ConsumerRecords<String, String> records = consumer.poll(1000);
                // 11. poll()은 레코드 전체를 리턴하고, 하나의 메시지만 가져오는 것이 아니므로 반복문 처리
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                // 12. 현재 배치를 통해 읽은 모든 메시지들을 처리한 후, 추가 메시지를 폴링하기 전 현재의 오프셋을 동기 커밋
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 12. 컨슈머 종료
            consumer.close();
        }
    }
}
