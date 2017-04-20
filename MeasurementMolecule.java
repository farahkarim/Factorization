package LLDFactorizationPackage;

import org.apache.jena.rdf.model.Resource;

public class MeasurementMolecule {
	private final Float value;
	private final Resource uom;

	MeasurementMolecule(final Float value, final Resource uom) {
		this.value = value;
		this.uom = uom;
	}

	public Float getValue() {
		return value;
	}

	public Resource getUOM() {
		return uom;
	}
}
