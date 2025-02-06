package it.quantyca.OTELCustomCollector.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import it.quantyca.OTELCustomCollector.model.Metric;
import it.quantyca.OTELCustomCollector.model.MetricID;

@Repository
public interface MetricRepository extends JpaRepository<Metric, MetricID> {
}
