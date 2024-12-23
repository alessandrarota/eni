package it.quantyca.OTELCustomCollector.repository;

import it.quantyca.OTELCustomCollector.model.Metric;
import it.quantyca.OTELCustomCollector.model.MetricID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MetricRepository extends JpaRepository<Metric, MetricID> {
}
