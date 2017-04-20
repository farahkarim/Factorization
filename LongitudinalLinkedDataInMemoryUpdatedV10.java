/* This class contains Factorization code with following specs:

 * 1. All Molecules are written in separate files
 * 2. During factorization these files are read for existing molecules n written for new ones
 * 3. To connect observation molecule with all observation URLs, lld:hasObservation predicates is added and rdf:sequence is removed as done in previous code
 * 4. Time Molecule is removed to preserve sequence of incoming observations
 * 5. Function addOriginalData() is removed to ensure consistency in vocabulary of factorized data, so that OPTIONAL/ UNION clasue in queries can be avoided
 * 6. Make efficient by running removing system.out.print() statements.
 * TruthDataFactorization  code is also run along with float value data factorization to make it efficient so for truth data input data is not read again
 * 7. Problem of a Value Molecule with two distinct values (0.21 and 236.59) is removed in this version.
 * this version removes double value molecule problem.
 * This problem is removed in getValueMolecule() function where the URL of new molecule is created by concatenating phenomenon by the actual value,
 *  instead of string "Value".
 * Program is made efficient by running phenomenaOrder query once on all the data at once.
 * Previously the query was running per sensor so was taking lot of time and this is a group query which is very expensive.
 * 8. runTimeMeasureQuery() function was running a CONSTRUCT SPARQL query which was adding link between observation URL and measurement, and time URIs.
 * This query was very expensive so we directly created RDF triples representing these links and are added to the factorized dataset.
 * 9. Optimizations are done to run factorization faster***/

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
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.FileManager;

public class LongitudinalLinkedDataInMemoryUpdatedV10 {

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
			out.close();
			in.close();
		}
		System.out.println("factorizationTime........." + factorizationTime);
	}

	public static void factorize() throws ParseException, IOException {
		// final long startTime = System.currentTimeMillis();
		final ResultSet results = getObservation();
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
							qs.get("time"));
			System.out.println(qs.get("time"));

			createMolecules(qs.getLiteral("v").getFloat(),
					qs.getResource("?u"), qs.getResource("procedure"),
					qs.getResource("property"), qs.getResource("phenomenon"),
					qs.getResource("ob"));
			// final long endTime = System.currentTimeMillis();
			// final long totalTime = endTime - startTime;
			// LongitudinalFactorization.startTime_Factorize =
			// LongitudinalFactorization.startTime_Factorize
			// + totalTime;
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
			final Resource phenomenon, final Resource observation) {
		final measurementClass measurement = new measurementClass(value, uom);
		Resource mURI = LongitudinalFactorization.mapMeasurement
				.get(measurement);
		if (mURI != null) {
			final observationClass obs = new observationClass(sensor, property,
					phenomenon, mURI);
			final Resource obsURI = LongitudinalFactorization.mapObservation
					.get(obs);
			if (obsURI != null) {
				LongitudinalFactorization.reducedModel
				.add(obsURI,
						LongitudinalFactorization.reducedModel
						.createProperty("http://linkeddata.com/ontology#hasObservation"),
						observation);
			} // / end of if (obsURI != null)
			else {
				createObservationMolecule(obs, observation);
			}
		} // / end of if (mURI != null)
		else {
			mURI = createMeasurmentMolecule(measurement);
			final observationClass obs = new observationClass(sensor, property,
					phenomenon, mURI);
			createObservationMolecule(obs, observation);
		}
	}

	// //////////////////////////////////////////////////////////////////////

	public static Resource createMeasurmentMolecule(
			final measurementClass measurement) {
		// final long startTime = System.currentTimeMillis();
		final String valueStr[] = String.valueOf(measurement.getValue()).split(
				"\\.");
		final Resource new_mURI = LongitudinalFactorization.reducedModel
				.createResource("http://linkeddata.com/ontology#"
						+ measurement.getUOM().getLocalName() + valueStr[0]
						+ valueStr[1]);
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
			final Resource observation) {
		// final long startTime = System.currentTimeMillis();
		final Resource new_obsURI = LongitudinalFactorization.reducedModel
				.createResource("http://linkeddata.com/ontology#"
						+ observation.getLocalName());
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

		// final long endTime = System.currentTimeMillis();
		// final long totalTime = endTime - startTime;
		// LongitudinalFactorization.startTime_createObservationMolecule =
		// LongitudinalFactorization.startTime_createObservationMolecule
		// + totalTime;
	}
}
