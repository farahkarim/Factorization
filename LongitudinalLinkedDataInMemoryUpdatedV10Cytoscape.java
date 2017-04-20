package LLDFactorizationPackage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.FileManager;

public class LongitudinalLinkedDataInMemoryUpdatedV10Cytoscape {

	// private static int lineCounter = 0;

	public static void singleFileFactorization(
			final String path_to_original_data,
			final String path_to_reduced_data) throws ParseException,
			IOException {
		long factorizationTime = 0;
		String inputFileName = "", outputFileName = "";
		final File folder = new File(path_to_original_data);
		final File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			LongitudinalFactorization.reducedModel
					.setNsPrefix("om-owl",
							"http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");
			LongitudinalFactorization.reducedModel.setNsPrefix("rdfs",
					"http://www.w3.org/2000/01/rdf-schema#");
			LongitudinalFactorization.reducedModel.setNsPrefix("sens-obs",
					"http://knoesis.wright.edu/ssw/");
			LongitudinalFactorization.reducedModel.setNsPrefix("owl-time",
					"http://www.w3.org/2006/time#");
			LongitudinalFactorization.reducedModel.setNsPrefix("owl",
					"http://www.w3.org/2002/07/owl#");
			LongitudinalFactorization.reducedModel.setNsPrefix("xsd",
					"http://www.w3.org/2001/XMLSchema#");
			LongitudinalFactorization.reducedModel.setNsPrefix("weather",
					"http://knoesis.wright.edu/ssw/ont/weather.owl#");
			LongitudinalFactorization.reducedModel.setNsPrefix("rdf",
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			LongitudinalFactorization.reducedModel.setNsPrefix("lld",
					"http://linkeddata.com/ontology#");
			inputFileName = outputFileName = listOfFiles[i].getName();
			inputFileName = path_to_original_data + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			LongitudinalFactorization.originalModel.read(in, null, "N3");
			final long startTime = System.currentTimeMillis();
			factorize();
			TruthDataClassV10.factorize();
			final long endTime = System.currentTimeMillis();
			final long totalTime = endTime - startTime;
			factorizationTime = factorizationTime + totalTime;
			LongitudinalFactorization.originalModel.removeAll();
			final OutputStream out = new FileOutputStream(path_to_reduced_data
					+ outputFileName);
			LongitudinalFactorization.reducedModel.write(out, "N3");
			LongitudinalFactorization.reducedModel.removeAll();
			// out.close();
			in.close();
		}
		System.out.println("factorizationTime........." + factorizationTime);
	}

	public static void factorize() throws ParseException, IOException {
		// final long startTime = System.currentTimeMillis();
		final ResultSet results = getObservation();
		while (results.hasNext()) {
			final QuerySolution qs = results.nextSolution();

			createMolecules(qs.getLiteral("v").getFloat(),
					qs.getResource("?u"), qs.getResource("procedure"),
					qs.getResource("property"), qs.getResource("phenomenon"),
					qs.getResource("ob"), qs.getResource("result"),
					qs.getResource("samplingTime"), qs.get("time"));

		}

		// /////////////////////////////
	}// end function LLDReduction(final String phenomena)

	public static ResultSet getObservation() throws FileNotFoundException {
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
				+ " ?result om-owl:uom ?u ; " + " om-owl:floatValue ?v . } ";

		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory.create(query,
				LongitudinalFactorization.originalModel);
		final ResultSet result = qexec.execSelect();

		// final long endTime = System.currentTimeMillis();
		// final long totalTime = endTime - startTime;
		// LongitudinalFactorization.startTime_phenomenonOrderV3 =
		// LongitudinalFactorization.startTime_phenomenonOrderV3
		// + totalTime;
		return result;
	}

	// ////////////////////////////////////////////////////////////////////

	public static void createMolecules(final Float value, final Resource uom,
			final Resource sensor, final Resource property,
			final Resource phenomenon, final Resource observation,
			final Resource result, final Resource samplingTime,
			final RDFNode time) {

		final measurementClass measurement = new measurementClass(value, uom);
		Resource mURI = LongitudinalFactorization.mapMeasurement
				.get(measurement);

		if (mURI != null) {
			final int mCounter = LongitudinalFactorization.mapMeasurementCounterCytoscape
					.get(mURI);
			LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
					.toString(), "value", value.toString(), mCounter));
			LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
					.toString(), "uom", uom.toString(), mCounter));
			LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
					.toString(), "rdf:type", "Measurement", mCounter));
			final observationClass obs = new observationClass(sensor, property,
					phenomenon, mURI);
			final Resource obsURI = LongitudinalFactorization.mapObservation
					.get(obs);
			if (obsURI != null) {
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

				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(obsURI.toString(),
								"hasObservation", observation.toString(),
								mCounter));
				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(observation.toString(),
								"result", result.toString(), mCounter));
				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(observation.toString(),
								"samplingTime", samplingTime.toString(),
								mCounter));
				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(samplingTime.toString(),
								"rdf:type", "Instant", mCounter));
				LongitudinalFactorization.linesFactorized
						.add(new LineCSVCytoscape(samplingTime.toString(),
								"time", time.toString(), mCounter));

				LongitudinalFactorization.reducedModel
						.add(observation,
								LongitudinalFactorization.reducedModel
										.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result"),
								result);

				LongitudinalFactorization.reducedModel
						.add(observation,
								LongitudinalFactorization.reducedModel
										.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#samplingTime"),
								samplingTime);

				LongitudinalFactorization.reducedModel
						.add(samplingTime,
								LongitudinalFactorization.reducedModel
										.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
								LongitudinalFactorization.reducedModel
										.createResource("http://www.w3.org/2006/time#Instant"));

				LongitudinalFactorization.reducedModel
						.add(samplingTime,
								LongitudinalFactorization.reducedModel
										.createProperty("http://www.w3.org/2006/time#inXSDDateTime"),
								time);

				LongitudinalFactorization.reducedModel
				.add(obsURI,
						LongitudinalFactorization.reducedModel
						.createProperty("http://linkeddata.com/ontology#hasObservation"),
						observation);
			} // / end of if (obsURI != null)
			else {
				createObservationMolecule(obs, observation, result,
						samplingTime, time);

			}
		} // / end of if (mURI != null)
		else {
			mURI = createMeasurmentMolecule(measurement, result);
			final observationClass obs = new observationClass(sensor, property,
					phenomenon, mURI);
			createObservationMolecule(obs, observation, result, samplingTime,
					time);
		}

	}

	// //////////////////////////////////////////////////////////////////////

	public static Resource createMeasurmentMolecule(
			final measurementClass measurement, final Resource result) {
		// final long startTime = System.currentTimeMillis();
		final String valueStr[] = String.valueOf(measurement.getValue()).split(
				"\\.");
		final Resource new_mURI = LongitudinalFactorization.reducedModel
				.createResource("http://linkeddata.com/ontology#"
						+ measurement.getUOM().getLocalName() + valueStr[0]
								+ valueStr[1]);

		// final int mCounter = LongitudinalFactorization.mMoleculeCounter++;
		//
		// LongitudinalFactorization.mapMeasurementCounterCytoscape.put(new_mURI,
		// mCounter);
		//
		// LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
		// .toString(), "value", measurement.getValue().toString(),
		// mCounter));
		// LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
		// .toString(), "uom", measurement.getUOM().toString(), mCounter));
		// LongitudinalFactorization.lines.add(new LineCSVCytoscape(result
		// .toString(), "rdf:type", "Measurement", mCounter));
		//
		// LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
		// new_mURI.toString(), "value",
		// measurement.getValue().toString(), mCounter));
		// LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
		// new_mURI.toString(), "uom", measurement.getUOM().toString(),
		// mCounter));
		// LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
		// new_mURI.toString(), "rdf:type", "Measurement", mCounter));

		LongitudinalFactorization.mapMeasurement.put(measurement, new_mURI);
		LongitudinalFactorization.reducedModel
				.add(new_mURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						LongitudinalFactorization.reducedModel
								.createResource("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#MeasureData"));

		LongitudinalFactorization.reducedModel
				.add(new_mURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue"),
						LongitudinalFactorization.reducedModel
								.createTypedLiteral(measurement.getValue()));

		LongitudinalFactorization.reducedModel
				.add(new_mURI,
						LongitudinalFactorization.reducedModel
								.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#uom"),
						measurement.getUOM());
		return new_mURI;
	}

	// ///////////////////////////////////////////////////////////////////////////

	public static void createObservationMolecule(final observationClass obs,
			final Resource observation, final Resource result,
			final Resource samplingTime, final RDFNode time) {
		final int mCounter = LongitudinalFactorization.mapMeasurementCounterCytoscape
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

		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "sensor", obs.getSensor().toString(),
				mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(obs
				.getSensor().toString(), "sensedBy", new_obsURI.toString(),
				mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "property",
				obs.getProperty().toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "phenomena", obs.getPhenomenon()
						.toString(), mCounter));
		LongitudinalFactorization.linesFactorized.add(new LineCSVCytoscape(
				new_obsURI.toString(), "result", obs.getmURI().toString(),
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
		LongitudinalFactorization.reducedModel
		.add(observation,
				LongitudinalFactorization.reducedModel
				.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#result"),
				result);

		LongitudinalFactorization.reducedModel
		.add(observation,
				LongitudinalFactorization.reducedModel
				.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#samplingTime"),
				samplingTime);

		LongitudinalFactorization.reducedModel
		.add(samplingTime,
				LongitudinalFactorization.reducedModel
				.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				LongitudinalFactorization.reducedModel
				.createResource("http://www.w3.org/2006/time#Instant"));

		LongitudinalFactorization.reducedModel
		.add(samplingTime,
				LongitudinalFactorization.reducedModel
				.createProperty("http://www.w3.org/2006/time#inXSDDateTime"),
				time);
	}
}
