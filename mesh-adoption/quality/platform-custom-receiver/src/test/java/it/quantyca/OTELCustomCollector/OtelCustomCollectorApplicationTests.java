package it.quantyca.OTELCustomCollector;

import it.quantyca.OTELCustomCollector.model.Metric;
import it.quantyca.OTELCustomCollector.model.MetricID;
import it.quantyca.OTELCustomCollector.repository.MetricRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;


@SpringBootTest
class OtelCustomCollectorApplicationTests {

	@Autowired
	private MetricRepository metricRepository;

	@Test
	void testConnectionToMetricRepository() throws Exception {
		Optional<Metric> queryResult = metricRepository.findById(
				new MetricID()
		);

		assertTrue(queryResult.isEmpty());
	}

}
