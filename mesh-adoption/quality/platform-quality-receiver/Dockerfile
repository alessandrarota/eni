# Etapa di build: crea l'artefatto jar
FROM maven:3.9.4-eclipse-temurin-21 as builder
WORKDIR /app
COPY . .
RUN mvn clean package spring-boot:repackage -DskipTests

# Etapa di esecuzione: esegue l'applicazione
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN apt-get update && apt-get install -y netcat

# Copia il jar generato
COPY --from=builder /app/target/*.jar platform-quality-receiver.jar

# Aggiungi lo script di attesa per la connessione al database
COPY wait-for-it.sh /wait-for-it.sh
RUN chmod +x /wait-for-it.sh

# Comando di avvio dell'applicazione Spring Boot
ENTRYPOINT ["/wait-for-it.sh", "host.docker.internal:1433", "--", "java", "-jar", "platform-quality-receiver.jar"]
