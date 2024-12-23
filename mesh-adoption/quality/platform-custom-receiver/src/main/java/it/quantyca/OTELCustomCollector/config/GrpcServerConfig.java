package it.quantyca.OTELCustomCollector.config;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.quantyca.OTELCustomCollector.service.OTELLogsService;
import it.quantyca.OTELCustomCollector.service.OTELMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcServerConfig {
    private Logger logger = LoggerFactory.getLogger(GrpcServerConfig.class);

    @Value("${grpc.server.port:1437}")
    private int GRPC_SERVER_PORT;

    @Bean
    public Server grpcServer(
            OTELMetricsService metricsService,
            OTELLogsService logsService
    ) throws Exception {
        Server server = ServerBuilder.forPort(GRPC_SERVER_PORT)
                .addService(metricsService)
                .addService(logsService)
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        logger.info("Starting OTLP server...");
        server.start();
        logger.info("gRPC server started on port: " + GRPC_SERVER_PORT);
        server.awaitTermination();
        return server;
    }
}