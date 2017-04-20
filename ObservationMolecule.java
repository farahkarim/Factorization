package LLDFactorizationPackage;

import org.apache.jena.rdf.model.Resource;

public class ObservationMolecule {
	private final Resource sensor;
	private final Resource property;
	private final Resource phenomenon;

	ObservationMolecule(final Resource sensor, final Resource property,
			final Resource phenomenon) {
		this.sensor = sensor;
		this.property = property;
		this.phenomenon = phenomenon;
	}

	public Resource getSensor() {
		return sensor;
	}

	public Resource getProperty() {
		return property;
	}

	public Resource getPhenomenon() {
		return phenomenon;
	}

}
