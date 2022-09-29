package com.oasis.messageproducer.route;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ProducerRoute extends RouteBuilder {


    @Override
    public void configure() throws Exception {
        from("kafka:test-sms?brokers=localhost:9092&groupId=group1").to("log:test-sms")
                .unmarshal().json(JsonLibrary.Jackson , Map.class)
                .setHeader("notificationId", simple("${body['NOTIFICATION_ID']}"))
                .log("Message received from Kafka : ${body}")
                .log("    on the topic ${headers[kafka.TOPIC]}")
                .log("    on the partition ${headers[kafka.PARTITION]}")
                .log("    with the offset ${headers[kafka.OFFSET]}")
                .log("    with the key ${headers[kafka.KEY]}")
                .to("sql:update oasis_notifications SET status='C' where notification_id=:#notificationId");

//         .process(exchange -> {
//            String body = exchange.getIn().getBody(String.class);
//            Map<String, String> bodyAsMap = Arrays.stream(body.replace("{", "").replace("}", "").split(","))
//                    .map(s -> s.split("="))
//                    .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1]));
//            exchange.getIn().setBody(bodyAsMap, Map.class);
//        })
    }
}
