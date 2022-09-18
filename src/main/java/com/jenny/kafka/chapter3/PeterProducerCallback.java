package com.jenny.kafka.chapter3;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

// 1. 콜백을 사용하기 위해 org.apache.kafka.clients.producer.Callback을 구현하는 클래스가 필요함
public class PeterProducerCallback implements Callback {
    private ProducerRecord<String, String> record;

    public PeterProducerCallback(ProducerRecord<String, String> record) {
        this.record = record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (e != null) {
            // 2. 카프카가 오류를 리턴하면 onCompletion()은 예외를 갖게 되며, 실제 운영 환경에서는 추가적인 예외 처리가 필요함
            e.printStackTrace();
        } else {
            System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Received Message: %s\n", metadata.topic(), metadata.partition()
                    , metadata.offset(), record.key(), record.value());
        }
    }
}
