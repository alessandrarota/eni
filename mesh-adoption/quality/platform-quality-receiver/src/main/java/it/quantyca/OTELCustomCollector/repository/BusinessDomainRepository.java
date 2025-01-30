package it.quantyca.OTELCustomCollector.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Repository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Repository
public class BusinessDomainRepository {
    @Getter
    private Map<String, String> businessDomains;

    private static Logger logger = LoggerFactory.getLogger(BusinessDomainRepository.class);

    @Value("${bd.credentials.tenant}")
    private String BD_TENANT;
    @Value("${bd.credentials.username}")
    private String BD_USERNAME;
    @Value("${bd.credentials.password}")
    private String BD_PASSWORD;

    public BusinessDomainRepository() {
        this.businessDomains = new HashMap<String, String>();
    }

    @PostConstruct
    public void init() {
        refreshData();
    }

    public String login() {
        if (BD_USERNAME == null || BD_PASSWORD == null || BD_USERNAME.isBlank() || BD_PASSWORD.isBlank()) {
            return null;
        }

        WebClient webClient = WebClient.builder()
                .baseUrl("https://app.blindata.io/auth/login")
                .build();

        Map tokenResponse = webClient.post()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "username", BD_USERNAME,
                        "password", BD_PASSWORD
                ))
                .retrieve()
                .onStatus(
                    HttpStatusCode::isError, clientResponse -> {
                            logger.warn("Client error on login: {}", clientResponse.statusCode());
                            return Mono.empty();
                        }
                )
                .bodyToMono(Map.class)
                .block();

        return tokenResponse != null ? (String) tokenResponse.get("access_token") : null;
    }

    @Scheduled(fixedRateString = "${bd.business_domain.refresh_rate_ms:900000}")
    public void refreshData() {
        logger.debug("Update BD from Blindata...");

        String accessToken = login();
        if (accessToken == null) {
            logger.warn("Access Token is null. Could not send request.");
        } else {
            WebClient webClient = WebClient.builder()
                    .baseUrl("https://app.blindata.io/api/v1/dataproductsdomains")
                    .defaultHeader("Authorization", "Bearer " + accessToken)
                    .defaultHeader("X-Bd-Tenant", BD_TENANT)
                    .build();

            String response = webClient.get()
                    .retrieve()
                    .onStatus(
                            HttpStatusCode::isError, clientResponse -> {
                                logger.warn("Client error on refreshData: {}", clientResponse.statusCode());
                                return Mono.empty();
                            }
                    )
                    .bodyToMono(String.class)
                    .block();

            Map<String, String> newBusinessDomainsMap = new HashMap<String, String>();

            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(response);
                if (node.get("content").isArray()) {
                    for (JsonNode bd : node.get("content")) {
                        newBusinessDomainsMap.put(
                                bd.get("name").asText().toUpperCase(),
                                bd.get("name").asText()
                        );
                    }
                } else {
                    logger.warn("Error on refreshData, content is not a list");
                }

                businessDomains = newBusinessDomainsMap;
            } catch (JsonProcessingException e) {
                logger.warn("Error on refreshData, could not find field {} in response", e.getLocation());
            }
        }
    }
}

