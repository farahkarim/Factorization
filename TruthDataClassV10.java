//*****9. Optimizations are done to run factorization faster*****/

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

public class TruthDataClassV10 {

	public static void factorize() throws ParseException, IOException {
		// final long startTime = System.currentTimeMillis();
		final ResultSet results = getTruthObservation();
		while (results.hasNext()) {
			final QuerySolution qs = results.nextSolution();
			LongitudinalFactorization.reducedModel
			.add(qs.getResource("ob"),
					LongitudinalFactorization.reducedModel
					.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result"),
					qs.getResource("result"));
			LongitudinalFactorization.reducedModel
			.add(qs.getResource("ob"),
					LongitudinalFactorization.reducedModel
					.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#samplingTime"),
					qs.getResource("samplingTime"));
			LongitudinalFactorization.reducedModel
			.add(qs.getResource("samplingTime"),
					LongitudinalFactorization.reducedModel
					.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
					LongitudinalFactorization.reducedModel
									.createResource("http://www.w3.org/2006/time#Instant"));
			LongitudinalFactorization.reducedModel
			.add(qs.getResource("samplingTime"),
					LongitudinalFactorization.reducedModel
					.createProperty("http://www.w3.org/2006/time#inXSDDateTime"),
					qs.getLiteral("time"));
			final String split[] = qs.get("v").toString().split("\\^");

			createMolecules(Boolean.valueOf(split[0]),
					qs.getResource("procedure"), qs.getResource("property"),
					qs.getResource("phenomenon"), qs.getResource("ob"),
					qs.getResource("result"), qs.getResource("samplingTime"),
					qs.get("time"));

		}

	}// end function LLDReduction(final String phenomena)

	public static ResultSet getTruthObservation() throws FileNotFoundException {
		/****
		 * The old version of factorization code was creating three molecules;
		 * observation, measurement and time molecule. But in new version we are
		 * no more creating time molecule. Therefore, we do not need to sort
		 * observations on time value. This function is modified to remove the
		 * retrieval based on time order.
		 ***/
		// final long startTime = System.currentTimeMillis();
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?ob ?v ?u ?property ?procedure ?result ?samplingTime ?time ?phenomenon "
				+ " WHERE { ?ob a ?phenomenon ; "
				+ " om-owl:procedure ?procedure ; "
				+ " om-owl:observedProperty ?property ; "
				+ " om-owl:result ?result ; "
				+ " om-owl:samplingTime ?samplingTime . "
				+ "	?samplingTime owl-time:inXSDDateTime ?time . "
				+ " ?result om-owl:booleanValue ?v . " + "} ";

		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory.create(query,
				LongitudinalFactorization.originalModel);
		final ResultSet result = qexec.execSelect();
		// final long endTime = System.currentTimeMillis();
		// final long totalTime = endTime - startTime;
		// LongitudinalFactorization.startTime_truthPhenomenonOrderV2 =
		// LongitudinalFactorization.startTime_truthPhenomenonOrderV2
		// + totalTime;
		return result;
	}

	// ////////////////////////////////////////////////////////////////////

	public static void createMolecules(final Boolean value,
			final Resource sensor, final Resource property,
			final Resource phenomenon, final Resource observation,
			final Resource result, final Resource samplingTime,
			final RDFNode time) {

		Resource mURI = LongitudinalFactorization.mapTruth.get(value);

		if (mURI != null) {

			final int mCounter = LongitudinalFactorization.mapTruthCounterCytoscape
					.get(mURI);
			LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
					.toString(), "value", value.toString(), mCounter));
			LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
					.toString(), "rdf:type", "TruthMeasurement", mCounter));

			// System.out.println("Truth Molecule found.....");// executes
			final observationClass obs = new observationClass(sensor, property,
					phenomenon, mURI);
			final Resource obsURI = LongitudinalFactorization.mapObservation
					.get(obs);
			if (obsURI != null) {

				// final int obsCounter =
				// LongitudinalFactorization.mapObservationCounterCytoscape
				// .get(obsURI);

				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						observation.toString(), "sensor", sensor.toString(),
						mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(sensor
						.toString(), "sensedBy", observation.toString(),
						mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						observation.toString(), "property",
						property.toString(), mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						observation.toString(), "phenomena", phenomenon
								.toString(), mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						observation.toString(), "result", result.toString(),
						mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						observation.toString(), "samplingTime", samplingTime
								.toString(), mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						samplingTime.toString(), "rdf:type", "Instant",
						mCounter));
				LongitudinalFactorization.lines.add(new LineCSVCytoscape(
						samplingTime.toString(), "time", time.toString(),
						mCounter));

				LongitudinalFactorization.reducedModel
						.add(obsURI,
								LongitudinalFactorization.reducedModel
										.createProperty("http://linkeddata.com/ontology#hasObservation"),
								observation);

				LongitudinalFactorization.linesFactorized
				.add(new LineCSVCytoscape(obsURI.toString(),
						"hasObservation", observation.toString(),
						mCounter));
				LongitudinalFactorization.linesFactorized
				.add(new LineCSVCytoscape(observation.toString(),
						"samplingTime", samplingTime.toString(),
						mCounter));
				LongitudinalFactorization.linesFactorized
				.add(new LineCSVCytoscape(observation.toString(),
						"result", result.toString(), mCounter));
				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(samplingTime.toString(),
								"rdf:type", "Instant", mCounter));
				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(samplingTime.toString(),
								"time", time.toString(), mCounter));
			} // / end of if (obsURI != null)
			else {
				// System.out.println("observation Molecule NOT found.....");///executes
				createObservationMolecule(obs, observation, result,
						samplingTime, time);
			}
		} // / end of if (mURI != null)
		else {
			// System.out.println("Truth Molecule NOT found.....");// executes
			mURI = createTruthMolecule(value, result);
			final observationClass obs = new observationClass(sensor, property,
					phenomenon, mURI);
			createObservationMolecule(obs, observation, result, samplingTime,
					time);
		}

	}

	// //////////////////////////////////////////////////////////////////

	public static Resource createTruthMolecule(final Boolean value,
			final Resource result) {
		// final long startTime = System.currentTimeMillis();
		final Resource new_mURI = LongitudinalFactorization.reducedModel
				.createResource("http://linkeddata.com/ontology#" + value);

		final int mCounter = LongitudinalFactorization.mMoleculeCounter++;
		LongitudinalFactorization.mapTruthCounterCytoscape.put(new_mURI,
				mCounter);
		LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
				.toString(), "value", value.toString(), mCounter));

		LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
				.toString(), "rdf:type", "TruthMeasurement", mCounter));

		LongitudinalFactorization.mapTruth.put(value, new_mURI);
		LongitudinalFactorization.reducedModel
				.add(new_mURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						LongitudinalFactorization.reducedModel
								.createResource("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#TruthData"));
		LongitudinalFactorization.reducedModel
				.add(new_mURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#booleanValue"),
						LongitudinalFactorization.reducedModel
								.createTypedLiteral(value));

		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_mURI.toString(), "rdf:type", "TruthData", mCounter));
		LongitudinalFactorization.linesFactorized
		.add(new LineCSVCytoscape(new_mURI.toString(), "booleanvalue",
				value.toString(), mCounter));

		return new_mURI;
	}

	// /////////////////////////////////////////////////////////////////////////////////
	public static void createObservationMolecule(final observationClass obs,
			final Resource observation, final Resource result,
			final Resource samplingTime, final RDFNode time) {
		final int mCounter = LongitudinalFactorization.mapTruthCounterCytoscape
				.get(obs.getmURI());
		final Resource new_obsURI = LongitudinalFactorization.reducedModel
				.createResource("http://linkeddata.com/ontology#"
						+ observation.getLocalName());

		LongitudinalFactorization.lines.add(new LineCSVCytoscape(observation
				.toString(), "sensor", obs.getSensor().toString(), mCounter));
		LongitudinalFactorization.lines.add(new LineCSVCytoscape(obs
				.getSensor().toString(), "sensedBy", observation.toString(),
				mCounter));
		LongitudinalFactorization.lines
		.add(new LineCSVCytoscape(observation.toString(), "property",
				obs.getProperty().toString(), mCounter));
		LongitudinalFactorization.lines.add(new LineCSVCytoscape(observation
				.toString(), "phenomena", obs.getPhenomenon().toString(),
				mCounter));
		LongitudinalFactorization.lines.add(new LineCSVCytoscape(observation
				.toString(), "result", result.toString(), mCounter));
		LongitudinalFactorization.lines
		.add(new LineCSVCytoscape(observation.toString(),
				"samplingTime", samplingTime.toString(), mCounter));
		LongitudinalFactorization.lines.add(new LineCSVCytoscape(samplingTime
				.toString(), "rdf:type", "Instant", mCounter));
		LongitudinalFactorization.lines.add(new LineCSVCytoscape(samplingTime
				.toString(), "time", time.toString(), mCounter));

		LongitudinalFactorization.mapObservation.put(obs, new_obsURI);
		LongitudinalFactorization.reducedModel
				.add(new_obsURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result"),
						obs.getmURI());
		LongitudinalFactorization.reducedModel
				.add(new_obsURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						obs.getPhenomenon());
		LongitudinalFactorization.reducedModel
				.add(new_obsURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#observedProperty"),
						obs.getProperty());
		LongitudinalFactorization.reducedModel
				.add(new_obsURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure"),
						obs.getSensor());
		LongitudinalFactorization.reducedModel
				.add(obs.getSensor(),
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation"),
						new_obsURI);
		LongitudinalFactorization.reducedModel
				.add(new_obsURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://linkeddata.com/ontology#hasObservation"),
						observation);

		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "result", obs.getmURI().toString(),
				mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "rdf:type", obs.getPhenomenon()
				.toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "property",
				obs.getProperty().toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "sensor", obs.getSensor().toString(),
				mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(obs
				.getSensor().toString(), "sensedBy", new_obsURI.toString(),
				mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "hasObservation",
				observation.toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				observation.toString(), "result", result.toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				observation.toString(), "samplingTime",
				samplingTime.toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				samplingTime.toString(), "rdf:type", "Instant", mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				samplingTime.toString(), "time", time.toString(), mCounter));

	}
}