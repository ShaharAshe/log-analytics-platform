package com.logpipe.ingest.controller;

import com.logpipe.ingest.dto.LogEvent;
import com.logpipe.ingest.service.LogProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class LogController {
    private  final LogProducer logProducer;
    public LogController(LogProducer logProducer) {
        this.logProducer = logProducer;
    }

    @PostMapping("/logs")
    public ResponseEntity<Void> sendLog(@RequestBody LogEvent logEvent) {
        logProducer.send(logEvent);
        // return 202 status and not 200 ok!
        return ResponseEntity.accepted().build();
    }
}
