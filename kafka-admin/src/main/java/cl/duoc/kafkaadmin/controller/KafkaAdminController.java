package cl.duoc.kafkaadmin.controller;

import cl.duoc.kafkaadmin.dto.PartitionsRequest;
import cl.duoc.kafkaadmin.dto.TopicConfigRequest;
import cl.duoc.kafkaadmin.dto.TopicRequest;
import cl.duoc.kafkaadmin.service.KafkaAdminService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/topics")
    public List<String> listarTopicos() {
        return service.listarTopicos();
    }

    @GetMapping("/topics/{name}")
    public Map<String, Object> detalleTopico(@PathVariable String name) {
        return service.detalleTopico(name);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> crearTopico(@RequestBody TopicRequest req) {
        service.crearTopico(req);
        return Map.of("mensaje", "Topico creado: " + req.name());
    }

    @PutMapping("/topics/{name}/config")
    public Map<String, String> modificarConfig(@PathVariable String name, @RequestBody TopicConfigRequest req) {
        service.modificarConfig(name, req);
        return Map.of("mensaje", "Configuracion actualizada: " + name);
    }

    @PutMapping("/topics/{name}/partitions")
    public Map<String, String> aumentarParticiones(@PathVariable String name, @RequestBody PartitionsRequest req) {
        service.aumentarParticiones(name, req);
        return Map.of("mensaje", "Particiones actualizadas: " + name);
    }

    @DeleteMapping("/topics/{name}")
    public Map<String, String> eliminarTopico(@PathVariable String name) {
        service.eliminarTopico(name);
        return Map.of("mensaje", "Topico eliminado: " + name);
    }

    @GetMapping("/groups")
    public List<String> listarGrupos() {
        return service.listarGrupos();
    }

    @GetMapping("/groups/{id}")
    public Map<String, Object> detalleGrupo(@PathVariable String id) {
        return service.detalleGrupo(id);
    }
}