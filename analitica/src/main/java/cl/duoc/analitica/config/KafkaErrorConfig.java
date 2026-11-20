package cl.duoc.analitica.config;

import cl.duoc.analitica.exception.EventoInvalidoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Manejo de errores: 2 reintentos (1 s entre cada uno) y luego se registra y se descarta. */
@Configuration
public class KafkaErrorConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean
    public DefaultErrorHandler errorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(
                (record, ex) -> log.error("Mensaje descartado topic={} particion={} offset={}: {}",
                        record.topic(), record.partition(), record.offset(), ex.getMessage()),
                new FixedBackOff(1000L, 2L));
        // un evento invalido no se reintenta
        handler.addNotRetryableExceptions(EventoInvalidoException.class);
        return handler;
    }
}