package com.inshorts.kafka.data;


import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaValueDeserializer implements Deserializer<SegmentBatchGenericEventDto> {

    private static final Gson gson = new Gson();

    private static final Logger LOG = LoggerFactory.getLogger(KafkaValueDeserializer.class);

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public SegmentBatchGenericEventDto deserialize(String topic, byte[] data) {
        try {
            return gson.fromJson(new String(data, StandardCharsets.UTF_8), SegmentBatchGenericEventDto.class);
        } catch (Exception e){
            LOG.error("Failed Record " + new String(data, StandardCharsets.UTF_8));
            return new SegmentBatchGenericEventDto();
        }
    }

    @Override
    public void close() {

    }
}