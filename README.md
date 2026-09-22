# Allgemein

Ziel ist es, die zugehörige PatientenID anhand einer Nexus-Befund-ID aus einem Kafka-Topic im DIZ Marburg zu ermitteln.

Hierzu werden alle Records beim Starten der Anwendung eingelesen, neu eingehende Records zu den vorhandenen Records
hinzugefügt und eine REST-Schnittstelle zum Abrufen der PatientenID anhand einer Nexus-Befund-ID bereitgestellt.

# Nutzung

Der folgende Aufruf führt zu der gezeigten beispielhaften Antwort.

```
curl -v http://.../?id=qgursxxxxxxxxxxxxxxxxx
```

```
HTTP/1.1 200 OK
content-length: 9
date: Tue, 22 Sep 2026 16:11:52 GMT

123456789
```

Im Falle einer nicht gefundenen Nexus-Befund-ID wird HTTP 404 zurückgegeben.

# Konfiguration

## Konsole-Parameter

```
Usage: nexus-pid [OPTIONS] --topic <TOPIC>

Options:
      --listen <LISTEN>
          Address and port for HTTP requests [env: LISTEN=] [default: [::]:3000]
      --bootstrap-servers <BOOTSTRAP_SERVERS>
          Kafka Bootstrap Server [env: KAFKA_BOOTSTRAP_SERVERS=] [default: kafka:9094]
      --topic <TOPIC>
          Kafka Topic [env: KAFKA_TOPIC=]
      --group-id <GROUP_ID>
          Kafka Group ID [env: KAFKA_GROUP_ID=] [default: nexis-pid]
      --ssl-ca-file <SSL_CA_FILE>
          CA file for SSL connection to Kafka [env: KAFKA_SSL_CA_FILE=]
      --ssl-cert-file <SSL_CERT_FILE>
          Certificate file for SSL connection to Kafka [env: KAFKA_SSL_CERT_FILE=]
      --ssl-key-file <SSL_KEY_FILE>
          Key file for SSL connection to Kafka [env: KAFKA_SSL_KEY_FILE=]
      --ssl-key-password <SSL_KEY_PASSWORD>
          The SSL key password [env: KAFKA_SSL_KEY_PASSWORD=]
```

## ENV-Vars und Docker-Compose

| Umgebungsvariable       | Bedeutung                                         |
|-------------------------|---------------------------------------------------|
| LISTEN                  | Adresse für die REST-Schnittstelle                |
| KAFKA_BOOTSTRAP_SERVERS | Kafka-Bootstrap-Server                            |
| KAFKA_TOPIC             | Kafka-Topic mit den entsprechenden Befund-Records |
| KAFKA_GROUP_ID          | Kafka-Group-ID (Standard: nexis-pid)              |
| KAFKA_SSL_CA_FILE       | Passwort                                          |
| KAFKA_SSL_CERT_FILE     | SSL-Zertifikat für Kafka (optional)               |
| KAFKA_SSL_KEY_FILE      | SSL-Key für Kafka (optional)                      |
| KAFKA_SSL_KEY_PASSWORD  | Passwort des SSL-Keys (optional)                  |
