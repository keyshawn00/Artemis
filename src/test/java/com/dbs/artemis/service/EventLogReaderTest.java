package com.dbs.artemis.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.URISyntaxException;

@SpringBootTest
public class EventLogReaderTest {

    @Autowired
    private EventLogReader eventLogReader;

    @Test
    void readTest() {
        eventLogReader.read();
    }
}
