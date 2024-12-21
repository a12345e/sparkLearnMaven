package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.util.*;

public class SampleFlatMapRowToJson implements FlatMapFunction<String, String> {

        @Override
        public Iterator<String> call(String json) {
            List<String> jsonStrings = new ArrayList<>();
            ObjectMapper objectMapper = new ObjectMapper();
            // Convert JSON string to Map<String, String>
            try {
                Map<String, Object> map = objectMapper.readValue(json, Map.class);
                Map<String, Object> map1 = new HashMap<>(map);
                map1.put("id", "k1");
                map1.put("value", 100);
                Map<String, Object> map2 = new HashMap<>(map);
                map2.put("id", "k2");
                map2.put("counter2", 200);

                jsonStrings.add(objectMapper.writeValueAsString(map1));
                jsonStrings.add(objectMapper.writeValueAsString(map2));

            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return jsonStrings.iterator();
        }
}
