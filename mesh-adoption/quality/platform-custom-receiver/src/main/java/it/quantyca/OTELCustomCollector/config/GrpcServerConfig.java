package it.quantyca.OTELCustomCollector.config;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.quantyca.OTELCustomCollector.service.OTELMetricsService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class GrpcServerConfig {
    private Logger logger = LoggerFactory.getLogger(GrpcServerConfig.class);

    @Value("${grpc.server.port:1437}")
    private int GRPC_SERVER_PORT;

    private Server server;
    private final ExecutorService grpcExecutor = Executors.newSingleThreadExecutor();

    @Bean
    public Server grpcServer(OTELMetricsService metricsService) {
        logger.info("Initializing gRPC server on port: {}", GRPC_SERVER_PORT);
        server = ServerBuilder.forPort(GRPC_SERVER_PORT)
                .addService(metricsService)
                .build();
        startGrpcServer();
        return server;
    }

    public void startGrpcServer() {
        grpcExecutor.execute(() -> {
            try {
                logger.info("Starting gRPC server on port: {}", GRPC_SERVER_PORT);
                server.start();
                logger.info("gRPC server started successfully.");
                server.awaitTermination();
            } catch (IOException | InterruptedException e) {
                logger.error("Error starting gRPC server", e);
                Thread.currentThread().interrupt();
            }
        });
    }

    @PreDestroy
    public void stopGrpcServer() {
        if (server != null) {
            logger.info("Shutting down gRPC server...");
            server.shutdown();
        }
        grpcExecutor.shutdown();
    }

}