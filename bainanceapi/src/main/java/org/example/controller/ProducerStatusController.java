package org.example.controller;

import org.example.model.ProducerStatus;
import org.example.repository.ProducerStatusRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/producers")
public class ProducerStatusController {

    private final ProducerStatusRepository repository;

    public ProducerStatusController(ProducerStatusRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/status")
    public List<ProducerStatus> getStatus() {
        return repository.findAll();
    }
}
