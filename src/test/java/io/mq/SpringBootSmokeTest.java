package io.mq;

import io.mq.client.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;
import io.mq.broker.Broker;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"mq.port=0", "mq.data-dir=${java.io.tmpdir}/mini-mq-test-${random.uuid}"})
class SpringBootSmokeTest {

    @Autowired Broker broker;
    @Autowired WebTestClient web;

    @Test
    void topicCreatedOverTheWireIsVisibleOnTheAdminApi() {
        try (Admin admin = new Admin("localhost", broker.port())) {
            admin.createTopic("smoke", 3);
        }
        web.get().uri("/admin/topics").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.smoke").isEqualTo(3);
        web.get().uri("/admin/topics/smoke").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(3);
    }
}
