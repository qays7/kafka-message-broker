package com.oasis.messagefactory.route;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class MessageRoute extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("timer:msgJob?fixedRate=true&period=5s&delay=5s")
                .to("sql:SELECT notification_id , notification_type , context_type , context_key FROM OASIS_NOTIFICATIONS WHERE STATUS='P' AND  ( scheduled_date is null OR scheduled_date < sysdate ) AND NOTIFICATION_TYPE = 'SMS' ORDER BY notification_id asc")
                .split(body()).streaming()
                .process(exchange -> {
                            Map body = exchange.getIn().getBody(Map.class);
                            String value = new ObjectMapper().writeValueAsString(body);
                            exchange.getIn().setBody(value);
                        }
                )
                .log("Filter Message with ${body}")
                .to("kafka:test-sms?brokers=localhost:9092&groupId=group1")
                .end();


    }

}
