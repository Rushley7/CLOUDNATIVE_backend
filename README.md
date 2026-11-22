Frontend (Angular + MSAL) -> API Gateway (JWT) -> backend (8080)
|
+---------------------------+---------------------------+
v v
RabbitMQ Kafka
pedidos.topic.exchange -> notificaciones.queue, tickets.queue topic pedidos-creados (3 part.)
inventario.direct.exchange -> inventario.queue |
(DLQ por cada cola) analitica (analitica-group)
| alerta si total >= 500000
v
topic pedidos-alertas
|
notificaciones (notificaciones-alertas-group)


Al crear un pedido, el backend lo guarda y publica el evento en RabbitMQ y en Kafka. Si falla Kafka, solo se registra en el log y el pedido no se pierde.

## Modulos

| Modulo | Puerto | Funcion |
|---|---|---|
| backend (raiz) | 8080 | API de pedidos. Productor RabbitMQ y Kafka |
| notificaciones | 8081 | Consume notificaciones.queue (RabbitMQ) y pedidos-alertas (Kafka) |
| tickets | 8082 | Consume tickets.queue (RabbitMQ) |
| rabbit-admin | 8083 | Administracion de RabbitMQ por REST (JWT) |
| inventario | 8084 | Consume inventario.queue (RabbitMQ) |
| analitica | 8085 | Consume pedidos-creados y publica en pedidos-alertas (Kafka) |
| kafka-admin | 8086 | Administracion de Kafka por REST (JWT) |

## Kafka

### Topicos

| Topico | Particiones | Replicas | Retencion | Cleanup policy | Productor | Consumidor (group) |
|---|---|---|---|---|---|---|
| pedidos-creados | 3 | 1 | 7 dias | delete | backend | analitica (`analitica-group`) |
| pedidos-alertas | 1 | 1 | 30 dias | delete | analitica | notificaciones (`notificaciones-alertas-group`) |

Se declaran en `KafkaConfig` del backend y se pueden modificar con kafka-admin. Un solo broker implica factor de replicacion 1 (solo laboratorio).

### Clave y orden
La clave del mensaje es el cliente: los pedidos de un mismo cliente van a la misma particion y conservan su orden.

### Consumer groups
- `analitica-group`: 3 hilos (`concurrency = 3`), uno por particion de `pedidos-creados`.
- `notificaciones-alertas-group`: lee `pedidos-alertas`. Es un grupo distinto, por lo que cada flujo avanza con su propio offset.

### Serializacion
DTO como records Java (`PedidoCreadoEvent`, `AlertaPedidoEvent`) serializados en JSON. Spring Boot 4 usa `JacksonJsonSerializer`/`JacksonJsonDeserializer` y el starter `spring-boot-starter-kafka`. Los consumidores usan `ErrorHandlingDeserializer`, y un mensaje que no es JSON no detiene al consumidor.

### Manejo de errores
- `DefaultErrorHandler` con 2 reintentos (1 s entre cada uno), luego se registra en el log y se descarta.
- `EventoInvalidoException` no se reintenta.
- Pruebas: producto `ERROR` (invalido, sin reintento) y `FALLA-TEMPORAL` (2 reintentos y descarte), igual que en RabbitMQ.
- La logica de negocio esta en servicios (`AnalisisService`, `AlertaService`), separada de los consumidores.

### kafka-admin (puerto 8086, requiere JWT)

| Metodo | Endpoint | Descripcion |
|---|---|---|
| GET | /api/kafka/topics | Lista topicos |
| GET | /api/kafka/topics/{name} | Particiones, replicas, retencion y cleanup policy |
| POST | /api/kafka/topics | Crea un topico |
| PUT | /api/kafka/topics/{name}/config | Modifica retencion y cleanup policy |
| PUT | /api/kafka/topics/{name}/partitions | Aumenta particiones |
| DELETE | /api/kafka/topics/{name} | Elimina un topico |
| GET | /api/kafka/groups | Lista consumer groups |
| GET | /api/kafka/groups/{id} | Miembros, offsets y lag |

Errores: 400 dato invalido, 404 no existe, 409 ya existe, 503 Kafka no disponible. Sin token valido responde 401.

## Infraestructura (docker-compose)

| Carpeta | Contenido | Puertos |
|---|---|---|
| docker/kafka | Zookeeper + broker Kafka (Confluent 7.5.0) | 9092 |
| docker/kafka-ui | Kafka UI (usa la red `kafka-net`) | 8090 |
| docker/rabbitmq | Cluster RabbitMQ de 2 nodos con politica ha-all | 5672/15672 y 5673/15673 |

Kafka UI usa el 8090 para no chocar con el backend (8080). Las credenciales de RabbitMQ se pueden cambiar con `RABBITMQ_USER` y `RABBITMQ_PASS`.

## Como levantar

Kafka (PowerShell, en este orden porque Kafka UI usa la red que crea el primer compose):

```powershell
cd docker\kafka
docker compose up -d
Start-Sleep -Seconds 30
cd ..\kafka-ui
docker compose up -d
```

Como `KAFKA_AUTO_CREATE_TOPICS_ENABLE` es false, los topicos se crean con el backend, con kafka-admin o con la CLI:

```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic pedidos-creados --partitions 3 --replication-factor 1 --config retention.ms=604800000 --config cleanup.policy=delete
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic pedidos-alertas --partitions 1 --replication-factor 1 --config retention.ms=2592000000 --config cleanup.policy=delete
```

RabbitMQ: `cd docker\rabbitmq` y `docker compose up -d`.

Microservicios (compilar y ejecutar):

```powershell
mvn -q -DskipTests -f analitica\pom.xml package
java -jar analitica\target\analitica-0.0.1-SNAPSHOT.jar
```

Lo mismo con `notificaciones`, `kafka-admin`, etc. La direccion de Kafka se configura con `KAFKA_BOOTSTRAP` (por defecto `localhost:9092`).

## Pruebas realizadas

Enviar eventos de prueba directamente al topico:

```powershell
'{"id":2,"cliente":"Ruben","producto":"Laptop","cantidad":10,"precio":100000.0,"estado":"PENDIENTE","fechaCreacion":"2026-11-20T10:01:00"}' | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:29092 --topic pedidos-creados
```

| Caso | Resultado |
|---|---|
| Pedido de monto alto (total >= 500000) | Alerta en pedidos-alertas y `[ALERTA ALTA]` en notificaciones |
| Pedido normal | Sin alerta |
| Producto `ERROR` | Descartado sin reintento |
| Producto `FALLA-TEMPORAL` | 2 reintentos y descarte |
| Mensaje que no es JSON | Descartado sin detener el consumidor |
| Pedido creado desde el frontend (EC2) | Evento en pedidos-creados, alerta del pedido 7 y lag 0 en ambos grupos |
| kafka-admin sin token o con token inventado | 401 |

Evidencias (capturas) en `docs/evidencias`.

## Seguridad
JWT de Entra ID. rabbit-admin y kafka-admin validan firma, issuer y audiencia (acepta el GUID de la app y `api://<GUID>`).

## Problemas conocidos y mejoras
- La IP publica de la EC2 cambia al reiniciar el laboratorio y hay que actualizar las integraciones del API Gateway (error 503). Mejora: IP elastica.
- La EC2 t3.micro (1 GB) queda justa con RabbitMQ, Kafka y los servicios Java. No levantar Kafka UI ni kafka-admin ahi durante la demo.
- Se usa Zookeeper porque la rubrica lo pide.
- El backend principal aun no valida la audiencia por su cuenta.

## Autores
Ruben Velasquez y Maximiliano Millacaris - DSY1107 Desarrollo Cloud Native I, Duoc UC.


git commit -m "docs: README con Kafka y RabbitMQ, y .gitattributes"
git push origin main
