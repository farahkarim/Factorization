package LLDFactorizationPackage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
//import com.hp.hpl.jena.query.Query;
//import com.hp.hpl.jena.query.QueryExecution;
//import com.hp.hpl.jena.query.QueryExecutionFactory;
//import com.hp.hpl.jena.query.QueryFactory;
//import com.hp.hpl.jena.query.QuerySolution;
//import com.hp.hpl.jena.query.ResultSet;
//import com.hp.hpl.jena.query.ResultSetFormatter;
//import com.hp.hpl.jena.rdf.model.Literal;
//import com.hp.hpl.jena.rdf.model.Model;
//import com.hp.hpl.jena.rdf.model.ModelFactory;
//import com.hp.hpl.jena.rdf.model.Resource;
//import com.hp.hpl.jena.rdf.model.Seq;
//import com.hp.hpl.jena.util.FileManager;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.util.FileManager;

public class LongitudinalFactorization {

	public static HashMap<Resource, Integer> mapMeasurementCounterCytoscape = new HashMap<Resource, Integer>();
	// public static HashMap<Resource, Integer> mapObservationCounterCytoscape =
	// new HashMap<Resource, Integer>();
	public static HashMap<Resource, Integer> mapTruthCounterCytoscape = new HashMap<Resource, Integer>();
	final static List<LineCSVCytoscape> lines = new ArrayList<LineCSVCytoscape>();
	final static List<LineCSVCytoscape> linesFactorized = new ArrayList<LineCSVCytoscape>();

	static int mMoleculeCounter = 0;
	static int obsMoleculeCounter = 1000;

	public static Map<Boolean, Resource> mapTruth = new HashMap<Boolean, Resource>();
	public static HashMap<measurementClass, Resource> mapMeasurement = new HashMap<measurementClass, Resource>();
	public static HashMap<observationClass, Resource> mapObservation = new HashMap<observationClass, Resource>();

	public static Map<Float, String> mapBlankNode = new HashMap<Float, String>(
			1000);
	private static Model ResultModel = ModelFactory.createDefaultModel();
	private static Model ReadModel = ModelFactory.createDefaultModel();
	private static int Count = 0;
	private static String obOrder = "a\trdf:Seq";
	private static String type = "";
	private static String property = "";
	private static String procedure = "";
	private static String uom = "";
	private static String dStart = null;
	private static String dEnd = null;
	private static float timeDiff = 0;
	private static int counter = 0;
	public static Model reducedModel = ModelFactory.createDefaultModel();
	public static Model originalModel = ModelFactory.createDefaultModel();
	public static Model RelativeHumidityObservationModel = ModelFactory
			.createDefaultModel();
	public static Model WindSpeedObservationModel = ModelFactory
			.createDefaultModel();
	public static Model TemperatureObservationModel = ModelFactory
			.createDefaultModel();
	public static Model WindDirectionObservationModel = ModelFactory
			.createDefaultModel();
	public static Model PressureObservationModel = ModelFactory
			.createDefaultModel();
	public static Model RainfallObservationModel = ModelFactory
			.createDefaultModel();
	public static Model VisibilityObservationModel = ModelFactory
			.createDefaultModel();
	public static Model SnowfallObservationModel = ModelFactory
			.createDefaultModel();
	public static Model PrecipitationObservationModel = ModelFactory
			.createDefaultModel();
	public static Model TimeModel = ModelFactory.createDefaultModel();

	public static long startTime_getPhenomena = 0;
	public static long startTime_phenomenonOrderV2 = 0;
	public static long startTime_phenomenonOrderV3 = 0;
	public static long startTime_runSelectSchemaQuery = 0;
	public static long startTime_getUOM = 0;
	public static long startTime_getValueMolecule = 0;
	public static long startTime_runTimeMeasureQuery = 0;
	public static long startTime_getTruthPhenomena = 0;
	public static long startTime_truthPhenomenonOrderV2 = 0;
	public static long startTime_truthPhenomenonOrder2 = 0;
	public static long startTime_substring = 0;
	public static long startTime_createMeasurmentMolecule = 0;
	public static long startTime_createObservationMolecule = 0;
	public static long startTime_createTMeasurmentMolecule = 0;
	public static long startTime_createTObservationMolecule = 0;
	public static long startTime_Read = 0;
	public static long startTime_Write = 0;
	public static long startTime_QuerySolution = 0;
	public static long startTime_TruthQuerySolution = 0;
	public static long startTime_TimeMeasure = 0;
	public static long startTime_Factorize = 0;
	public static long startTime_sorting = 0;

	public static void main(final String[] args) throws ParseException,
	IOException {
		final long startTime = System.currentTimeMillis();
		// final String path_to_original_data = args[0];
		// final String path_to_reduced_data = args[1];

		LongitudinalLinkedDataInMemoryUpdatedV10.singleFileFactorization(
						"/home/hadoop/Cytoscape_v3.4.0/RDFData/RDFFullFILES/RDFN3/",
				"/home/hadoop/Cytoscape_v3.4.0/RDFData/RDFFullFILES/RDFFactorized/");
		// generateOriginalCytoscapeCSV("/home/hadoop/Cytoscape_v3.4.0/RDFData/RDFFullFILES/RDFCSV/rdfOriginalcsvData.csv");
		// generateFactorizedCytoscapeCSV("/home/hadoop/Cytoscape_v3.4.0/RDFData/RDFFullFILES/RDFCSV/rdfFactorizedcsvData.csv");
		// countTriples("/home/hadoop/rdf2/", "/home/hadoop/streamRDF2/");
		// generateCSV();
		final long endTime = System.currentTimeMillis();
		final long totalTime = endTime - startTime;
		System.out.println("totalTime........." + totalTime);
	}

	public static void generateOriginalCytoscapeCSV(final String fileName) {
		// Delimiter used in CSV file
		// final String fileName =
		// System.getProperty("user.home")+"/student.csv";

		final String NEW_LINE_SEPARATOR = "\n";

		// CSV file header
		final Object[] FILE_HEADER = { "Subject", "Predicate", "Object",
				"MeasurementMoleculeNr" };
		FileWriter fileWriter = null;
		CSVPrinter csvFilePrinter = null;
		// Create the CSVFormat object with "\n" as a record delimiter
		final CSVFormat csvFileFormat = CSVFormat.DEFAULT
				.withRecordSeparator(NEW_LINE_SEPARATOR);
		try {
			// initialize FileWriter object
			fileWriter = new FileWriter(fileName);

			// initialize CSVPrinter object
			csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);

			// Create CSV file header
			csvFilePrinter.printRecord(FILE_HEADER);

			// Write a new student object list to the CSV file

			for (final LineCSVCytoscape line : lines) {
				final List lineDataRecord = new ArrayList();
				lineDataRecord.add(line.getSubject());
				lineDataRecord.add(line.getPredicate());
				lineDataRecord.add(line.getObject());
				lineDataRecord.add(line.getMeasurementMoleculeNr());
				// lineDataRecord.add(line.getObservationMoleculeNr());
				csvFilePrinter.printRecord(lineDataRecord);
			}

			System.out.println("CSV file was created successfully !!!");
		} catch (final Exception e) {

			System.out.println("Error in CsvFileWriter !!!");

			e.printStackTrace();

		} finally {

			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (final IOException e) {

				System.out
				.println("Error while flushing/closing fileWriter/csvPrinter !!!");

				e.printStackTrace();

			}

		}
	}

	public static void generateFactorizedCytoscapeCSV(final String fileName) {
		// Delimiter used in CSV file
		// final String fileName =
		// System.getProperty("user.home")+"/student.csv";

		final String NEW_LINE_SEPARATOR = "\n";

		// CSV file header
		final Object[] FILE_HEADER = { "Subject", "Predicate", "Object",
				"MeasurementMoleculeNr" };
		FileWriter fileWriter = null;
		CSVPrinter csvFilePrinter = null;
		// Create the CSVFormat object with "\n" as a record delimiter
		final CSVFormat csvFileFormat = CSVFormat.DEFAULT
				.withRecordSeparator(NEW_LINE_SEPARATOR);
		try {
			// initialize FileWriter object
			fileWriter = new FileWriter(fileName);

			// initialize CSVPrinter object
			csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);

			// Create CSV file header
			csvFilePrinter.printRecord(FILE_HEADER);

			// Write a new student object list to the CSV file

			for (final LineCSVCytoscape line : linesFactorized) {
				final List lineDataRecord = new ArrayList();
				lineDataRecord.add(line.getSubject());
				lineDataRecord.add(line.getPredicate());
				lineDataRecord.add(line.getObject());
				lineDataRecord.add(line.getMeasurementMoleculeNr());
				// lineDataRecord.add(line.getObservationMoleculeNr());
				csvFilePrinter.printRecord(lineDataRecord);
			}

			System.out.println("CSV file was created successfully !!!");
		} catch (final Exception e) {

			System.out.println("Error in CsvFileWriter !!!");

			e.printStackTrace();

		} finally {

			try {
				fileWriter.flush();
				fileWriter.close();
				csvFilePrinter.close();
			} catch (final IOException e) {

				System.out
				.println("Error while flushing/closing fileWriter/csvPrinter !!!");

				e.printStackTrace();

			}

		}
	}

	public static void generateCSV() throws IOException {
		String inputFileName;
		final File folder = new File("/home/hadoop/rdf12/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/rdf12/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			// System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			in.close();
			models.add(model);
			model.close();
		}// end of for (int i = 0; i < listOfFiles.length; i++)

		final OutputStream out = new FileOutputStream(
				"/home/hadoop/Cytoscape_v3.4.0/RDFData/rdf.csv");

		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?subject ?predicate ?object "
				+ " WHERE { "
				+ " ?subject ?predicate ?object } ";
		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}

	}

	public static void readFiles() throws ParseException, IOException {
		String inputFileName;
		final File folder = new File("/home/hadoop/rdf2/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/rdf/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			// System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			in.close();
			models.add(model);
			model.close();

		}// end of for (int i = 0; i < listOfFiles.length; i++)
		System.out.println("Data Loaded.");
		models.close();
	}

	public static void callLLDReduction() throws FileNotFoundException,
	ParseException {
		String inputFileName = "";
		final OutputStream out = new FileOutputStream(
				"C:/Users/karim/Documents/Projects/Sensor-Data-Storage/Dataset/rdf-reduced/rainfall/rainfall.n3");
		final File folder = new File(
				"C:/Users/karim/Documents/Projects/Sensor-Data-Storage/Dataset/rdf/test/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();
		final Model results = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "C:/Users/karim/Documents/Projects/Sensor-Data-Storage/Dataset/rdf/test/"
					+ inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
		}// end of for (int i = 0; i < listOfFiles.length; i++)
			// results.add(LLDReduction(models, "RelativeHumidity"));
			// results.add(LLDReduction(models, "WindSpeed"));
			// results.add(LLDReduction(models, "Temperature"));
			// results.add(LLDReduction(models, "WindDirection"));
			// results.add(LLDReduction(models, "Pressure"));
		results.add(LLDReduction(models, "Rainfall"));
		// results.add(LLDReduction(models, "Visibility"));
		// results.add(LLDReduction(models, "Snowfall"));
		// results.add(LLDReduction(models, "Precipitation"));
		results.write(out, "N3");
	}

	public static Model LLDReduction(final Model model, final String phenomenon)
			throws FileNotFoundException, ParseException {
		// Map<Float, String> mapBlankNode = new HashMap<Float, String>();
		int j;
		mapBlankNode.clear();
		final Boolean Equidistant = true;
		final List<String> obList = new ArrayList<String>();
		final List<Float> vList = new ArrayList<Float>();
		final List<Resource> order = new ArrayList<Resource>();
		boolean mapFound = false;
		final String inputFileName = "";
		final Model result = ModelFactory.createDefaultModel();
		// // retrieve observations in order of ascending time stamps
		// final ResultSet results= runSelectQuery(model, phenomenon);
		//
		// while (results.hasNext()) {
		// final QuerySolution qs = results.nextSolution();
		// obList.add(qs.getResource("ob").getLocalName());
		// // System.out.println("observation = " +
		// // qs.getResource("ob").getLocalName());
		// vList.add(qs.getLiteral("v").getFloat());
		// // System.out.println("value = " + qs.getLiteral("v").getFloat());
		// } // end while (results.hasNext())

		// / create a Hash Map for EACH unique value
		result.add(createValueMolecule(model, phenomenon));
		result.add(runConstructQuery(model, obList));
		if (obList.size() == 1) {
			System.out.println("one observation = " + obList.get(0) + "    "
					+ vList.get(0));
			result.add(addOriginalData(model, obList.get(0), vList.get(0)));
			Count++;
		}// end of if (obList.size() == 1)
		else {
			for (j = 0; j < (vList.size() - 1); j++) {
				if (vList.get(j).equals(vList.get(j + 1))) {
					// System.out.println(obList.get(i) + "....." +
					// vList.get(i));
					mapFound = true;
					counter++;
					// obOrder.add("http://knoesis.wright.edu/ssw/" +
					// obList.get(j));
					order.add(model
							.createResource("http://knoesis.wright.edu/ssw/"
									+ obList.get(j)));
					// obOrder = obOrder + ";rdf:_" + counter + "\tsens-obs:" +
					// obList.get(j);
					// /// Run CONSTRUCT query to write triples which show link
					// of observations with measurements and time stamps
					// result.add(runConstructQuery(model, obList.get(j)));
					if (counter == 1) {
						// // run select query to retrieve schema
						runSelectSchemaQuery(model, obList.get(j));
						// / run second SELECT query to retrieve time of first
						// observation
						dStart = runSelectTimeQuery(model, obList.get(j));
					}
					if (counter == 2) {
						// / run second SELECT query to retrieve second
						// observation time
						// String timesplit1[] = dStart.split("\\^");
						final Date firstDate = new SimpleDateFormat(
								"yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)
						.parse(dStart);
						final String Obs2Time = runSelectTimeQuery(model,
								obList.get(j));
						// String timesplit2[] = Obs2Time.split("\\^");
						final Date secondDate = new SimpleDateFormat(
								"yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)
						.parse(Obs2Time);
						final float diff = secondDate.getTime()
								- firstDate.getTime();
						// if (secondDate.getTime() - firstDate.getTime() == 0)
						// {
						// System.out.println("second= " + Obs2Time + " " +
						// secondDate.getTime());
						// System.out.println("first= " + dStart + " " +
						// firstDate.getTime());
						// System.out.println("diff= " + (secondDate.getTime() -
						// firstDate.getTime()));
						// }
						timeDiff = diff / 1000;
					}
				}// end if (vList.get(i).equals(vList.get(i + 1)))
				else {
					if (mapFound) {
						mapFound = false;
						counter++;
						Count++;
						if (counter == 2) {
							// / run second SELECT query to retrieve second
							// observation time
							// String timesplit1[] = dStart.split("\\^");
							final Date firstDate = new SimpleDateFormat(
									"yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)
							.parse(dStart);
							final String Obs2Time = runSelectTimeQuery(model,
									obList.get(j));
							// String timesplit2[] = Obs2Time.split("\\^");
							final Date secondDate = new SimpleDateFormat(
									"yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)
							.parse(Obs2Time);
							final float diff = secondDate.getTime()
									- firstDate.getTime();
							timeDiff = diff / 1000;
						}
						final Seq obOrder = result.createSeq("_:obOrderBN"
								+ Count);
						// /// Run CONSTRUCT query to write triples which show
						// link of observations with measurements and time
						// stamps
						// result.add(runConstructQuery(model, obList.get(j)));
						// / run second SELECT query to retrieve last
						// observation time
						dEnd = runSelectTimeQuery(model, obList.get(j));
						// obOrder = obOrder + ";rdf:_" + counter +
						// "\tsens-obs:" + obList.get(j);
						// obOrder.add("http://knoesis.wright.edu/ssw/" +
						// obList.get(j));
						order.add(model
								.createResource("http://knoesis.wright.edu/ssw/"
										+ obList.get(j))); // / ?????check model
						// or result

						if (Equidistant) {
							for (int k = 0; k < order.size(); k++) {
								obOrder.add(order.get(k));
							}
							// run CONSTRUCT query to create new Blank Node for
							// observations with same value
							result.add(ConstructEquidistantData(model,
									obList.get(j), vList.get(j)));
							// System.out.println("same Observations....." +
							// counter);
						}
						// result.createSeq("_:obOrderBN" + Count);

					}// end if(mapFound)
					else {
						result.add(addOriginalData(model, obList.get(j),
								vList.get(j)));
						Count++;
					}
					// result.createSeq("_:obOrderBN" + Count);
					order.clear();
					type = "";
					property = "";
					procedure = "";
					uom = "";
					dStart = null;
					dEnd = null;
					timeDiff = 0;
					counter = 0;
				}
			}// end for (int i = 0; i < vList.size() - 1; i++)
			if (mapFound == true) {
				mapFound = false;
				counter++;
				Count++;
				if (counter == 2) {
					// / run second SELECT query to retrieve second observation
					// time
					// String timesplit1[] = dStart.split("\\^");
					final Date firstDate = new SimpleDateFormat(
							"yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)
					.parse(dStart);
					final String Obs2Time = runSelectTimeQuery(model,
							obList.get(j));
					// String timesplit2[] = Obs2Time.split("\\^");
					final Date secondDate = new SimpleDateFormat(
							"yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH)
					.parse(Obs2Time);
					final float diff = secondDate.getTime()
							- firstDate.getTime();
					timeDiff = diff / 1000;
				}
				final Seq obOrder = result.createSeq("_:obOrderBN" + Count);
				// /// Run CONSTRUCT query to write triples which show link of
				// observations with measurements and time stamps
				// result.add(runConstructQuery(model, obList.get(j)));
				// / run second SELECT query to retrieve last observation time
				dEnd = runSelectTimeQuery(model, obList.get(j));
				// obOrder = obOrder + ";rdf:_" + counter + "\tsens-obs:" +
				// obList.get(j);
				// obOrder.add("http://knoesis.wright.edu/ssw/" +
				// obList.get(j));
				order.add(model.createResource("http://knoesis.wright.edu/ssw/"
						+ obList.get(j)));

				if (Equidistant) {
					for (int k = 0; k < order.size(); k++) {
						obOrder.add(order.get(k));
					}
					// run CONSTRUCT query to create new Blank Node for
					// observations with same value
					result.add(ConstructEquidistantData(model, obList.get(j),
							vList.get(j)));
					// System.out.println("same Observations....." + counter);
				}
				// result.createSeq("_:obOrderBN" + Count);
				order.clear();
				type = "";
				property = "";
				procedure = "";
				uom = "";
				dStart = null;
				dEnd = null;
				timeDiff = 0;
				counter = 0;

			} else if (j == (vList.size() - 1)) {
				result.add(addOriginalData(model, obList.get(j), vList.get(j)));
				Count++;
				order.clear();
				type = "";
				property = "";
				procedure = "";
				uom = "";
				dStart = null;
				dEnd = null;
				timeDiff = 0;
				counter = 0;
			}
		}// end od else if (obList.size() == 1)
			// System.out.println("Total Reduced Observations....." + Count);
			// ResultModel.write(out, "N3");
			// model.close();
			// }// end of for (int i = 0; i < listOfFiles.length; i++)
			// mapBlankNode.clear();
		return result;

	} // end function LLDReduction(final String phenomena)

	public static void countOriginalObservations()
			throws FileNotFoundException, ParseException {
		final int totalObs = 0;
		String inputFileName;
		final File folder = new File("/home/hadoop/rdf2/");
		final File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			// Model results = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/rdf2/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");

			// // Count total observations*************************
			final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
					+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
					+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
					+ "prefix owl-time: <http://www.w3.org/2006/time#> "
					+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
					+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
					+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
					+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
					+ " PREFIX lld: <http://linkeddata.com/ontology#> "
					+ " SELECT (count(?obs) as ?countObs) "
					+ " WHERE { ?obs om-owl:observedProperty ?property } ";

			final Query query = QueryFactory.create(queryString);
			final QueryExecution qexec = QueryExecutionFactory.create(query,
					model);
			final ResultSet result = qexec.execSelect();
			while (result.hasNext()) {
				final QuerySolution qs = result.nextSolution();
				System.out.println("# of Original Observation ="
						+ qs.getLiteral("countObs").getInt());

				// totalObs = totalObs
				// + Integer.parseInt(qs.getResource("?countObs")
				// .toString());

			}
		}
		// result.write(out, "N3");
		// System.out.println("# of Original Observation =" + totalObs);

	}

	public static void countFactorizedObservations()
			throws FileNotFoundException, ParseException {
		final int totalObs = 0;
		String inputFileName;
		final File folder = new File("/home/hadoop/streamRDF2/");
		final File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			// Model results = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/streamRDF2/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N-Triples");

			// // Count total observations*************************
			final String queryString = " SELECT (count(?o) as ?countObs) "
					+ " WHERE { ?obs <http://linkeddata.com/ontology#hasObservation> ?o } ";

			final Query query = QueryFactory.create(queryString);
			final QueryExecution qexec = QueryExecutionFactory.create(query,
					model);
			final ResultSet result = qexec.execSelect();
			while (result.hasNext()) {
				final QuerySolution qs = result.nextSolution();
				System.out.println("# ofFactorized Observation ="
						+ qs.getLiteral("countObs").getInt());
				// totalObs = totalObs
				// + Integer.parseInt(qs.getResource("?countObs")
				// .toString());

			}
		}
		// result.write(out, "N3");
		// System.out.println("# of Factorized Observation =" + totalObs);

	}

	public static void countUniqueValues() throws FileNotFoundException,
	ParseException {
		final Model models = ModelFactory.createDefaultModel();
		String inputFileName = "";
		final File folder = new File(
				"/home/hadoop/April2003-Temp-Measurements/");
		// final File folder = new
		// File("/home/hadoop/ISWC-2016/test-sort/date/");

		final OutputStream out = new FileOutputStream(
				"/home/hadoop/Temp-Unique-values.csv");
		// final OutputStream out = new FileOutputStream(
		// "/home/hadoop/ISWC-2016/TestQueriesResults/dateTempValuea.csv");
		final File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			// Model results = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/April2003-Temp-Measurements/"
					+ inputFileName;
			// inputFileName = "/home/hadoop/ISWC-2016/test-sort/date/"
			// + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
		}// end of for (int i = 0; i < listOfFiles.length; i++)
			// final String reducedQuery =
			// "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
			// + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			// + "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
			// + "prefix owl-time: <http://www.w3.org/2006/time#> "
			// + "prefix owl: <http://www.w3.org/2002/07/owl#> "
			// + "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
			// +
			// "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
			// + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			// + "prefix lld: <http://linkeddata.com/ontology#> "
			// + " SELECT ?v "
			// + " WHERE { "
			// + " { ?ob a weather:TemperatureObservation ; "
			// + " lld:hasResult ?m ."
			// + " ?m om-owl:floatValue ?v } "
			// + " UNION "
			// + " { ?ob a weather:TemperatureObservation ; "
			// + " lld:hasResult ?m ;"
			// + " lld:hasOrder ?x ."
			// + " ?m om-owl:floatValue ?v . "
			// + " ?x ?p ?elements. "
			// +
			// " FILTER (?p != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ) "
			// + "}"
			//
			// + "} " + " ORDER BY ASC(?v) ";
			// final String originalQuery =
			// "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
			// + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			// + "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
			// + "prefix owl-time: <http://www.w3.org/2006/time#> "
			// + "prefix owl: <http://www.w3.org/2002/07/owl#> "
			// + "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
			// +
			// "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
			// + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			// + "prefix lld: <http://linkeddata.com/ontology#> "
			// + " SELECT ?v "
			// + " WHERE { "
			// + "  ?ob a weather:TemperatureObservation ; "
			// + " om-owl:result ?m ."
			// + " ?m om-owl:floatValue ?v  "
			// + "} "
			// + " ORDER BY ASC(?v) ";

		final String originalQuery = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?v (COUNT(?v) as ?vCount)"
				+ " WHERE { "
				// + "  ?ob a weather:TemperatureObservation ; "
				+ " ?ob om-owl:result ?m ."
				+ " ?m om-owl:floatValue ?v  "
				+ "} " + " GROUP BY ?v HAVING (?vCount > 9) ";
		final Query query = QueryFactory.create(originalQuery);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
	}

	public static void countValueMolecules() throws FileNotFoundException,
	ParseException {
		final List<Resource> mList = new ArrayList<Resource>();
		final Model models = ModelFactory.createDefaultModel();
		String inputFileName;
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT DISTINCT ?m "
				+ " WHERE { ?ob lld:hasResult ?m "
				+ "} ";
		inputFileName = "C:/Users/karim/Documents/Projects/Sensor-Data-Storage/Dataset/rdf-reduced/test-value-molecule/690.n3";
		final InputStream in = FileManager.get().open(inputFileName);
		if (in == null)
			throw new IllegalArgumentException("File: " + inputFileName
					+ " not found");
		models.read(in, null, "N3");

		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		while (result.hasNext()) {
			final QuerySolution qs = result.nextSolution();
			mList.add(qs.getResource("m"));
		}
		System.out.println("# of value molecules=" + mList.size());

	}

	public static void getTimeMolecules() throws FileNotFoundException,
	ParseException {
		final List<Resource> mList = new ArrayList<Resource>();
		final Model models = ModelFactory.createDefaultModel();
		String inputFileName;
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?time ?start ?end "
				+ " WHERE { ?time a owl-time:Instant ; "
				+ " lld:endTime ?end ; "
				+ " lld:startTime ?start . "
				+ "} ORDER BY ?start ";
		final File folder = new File(
				"/home/hadoop/ISWC-2016/690-with-time-molecule/690-reduced/");

		final OutputStream out = new FileOutputStream(
				"/home/hadoop/time-molecule.csv");

		final File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/690-with-time-molecule/690-reduced/"
					+ inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
		}// end of for (int i = 0; i < listOfFiles.length; i++)

		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
	}

	public static void countTriples(final String path_to_original_data,
			final String path_to_reduced_data) throws FileNotFoundException,
			ParseException {
		String inputFileName = "";
		long originalCount = 0;
		long reduceCount = 0;

		File folder = new File(path_to_original_data);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = path_to_original_data + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			model.read(in, null, "N3");
			originalCount = originalCount + model.size();
		}
		folder = new File(path_to_reduced_data);
		listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = path_to_reduced_data + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			model.read(in, null, "N-Triples");
			reduceCount = reduceCount + model.size();
			System.out.println("# of triples after reduction per file ..= "
					+ model.size());
		}
		System.out
		.println("# of triples before reduction ..= " + originalCount);
		System.out.println("# of triples after reduction ..= " + reduceCount);

	}

	public static Model createValueMolecule(final Model model,
			final String phenomenon) {
		final Model molecule = ModelFactory.createDefaultModel();
		final List<Float> vList = new ArrayList<Float>();
		String BNname = "";
		int counter = 0;
		String uom = null;

		// / find unique values
		String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT DISTINCT ?v "
				+ " WHERE { ?ob a weather:"
				+ phenomenon
				+ "Observation ; "
				+ " om-owl:result ?m . "
				+ " ?m om-owl:floatValue ?v " + "} ";
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.create(query, model);
		ResultSet result = qexec.execSelect();
		while (result.hasNext()) {
			final QuerySolution qs = result.nextSolution();
			vList.add(qs.getLiteral("v").getFloat());
		}
		System.out.println("# of unique values for " + phenomenon + "  ="
				+ vList.size());

		for (int j = 0; j < vList.size(); j++) {
			if (mapBlankNode.get(vList.get(j)) != null) {
				System.out
				.println("Map and blank node Already exists for value "
						+ vList.size());
			}// end of if (mapBlankNode.get(vList.get(j)) == null)
			else {
				counter++;
				BNname = "_:m" + counter + phenomenon;
				final Literal v = molecule.createTypedLiteral(vList.get(j));
				queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
						+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
						+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
						+ "prefix owl-time: <http://www.w3.org/2006/time#> "
						+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
						+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
						+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
						+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
						+ " SELECT DISTINCT ?uom "
						+ " WHERE { ?ob a weather:"
						+ phenomenon
						+ "Observation ; "
						+ " om-owl:result ?result . "
						+ " ?result om-owl:floatValue \""
						+ vList.get(j)
						+ "\"^^xsd:float ;" + " om-owl:uom ?uom " + "} ";
				query = QueryFactory.create(queryString);
				qexec = QueryExecutionFactory.create(query, model);
				result = qexec.execSelect();
				if (result.hasNext()) {
					final QuerySolution qs = result.nextSolution();
					uom = qs.getResource("uom").getLocalName();
				}
				molecule.add(
						molecule.createResource(BNname),
						molecule.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						molecule.createResource("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#MeasureData"));
				// molecule.add(molecule.createResource(BNname),
				// molecule.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue"),
				// molecule.createLiteral(vList.get(j) +
				// "http://www.w3.org/2001/XMLSchema#float"));
				molecule.add(
						molecule.createResource(BNname),
						molecule.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#floatValue"),
						molecule.createTypedLiteral(vList.get(j)));
				molecule.add(
						molecule.createResource(BNname),
						molecule.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#uom"),
						molecule.createResource("http://knoesis.wright.edu/ssw/ont/weather.owl#"
								+ uom));
				mapBlankNode.put(vList.get(j), BNname);
			}// else
		}// end of for (int j = 0; j < vList.size() - 1; j++)
		System.out.println("# of blank nodes created for " + phenomenon + "  ="
				+ counter);
		return molecule;
	}

	public static void retrieveObservation() throws FileNotFoundException {
		/*** This function retrieves a particular observation. ***/
		String inputFileName = "";
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/PhD/Sensornets2017/Example-Observations/observation3.csv");
		final String SELECTQuery = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?type ?property ?procedure ?result ?samplingTime ?value ?uom ?time "
				+ " WHERE { sens-obs:Observation_AirTemperature_4UT01_2003_4_4_5_15_00 rdf:type ?type ; "
				+ " om-owl:observedProperty ?property ; "
				+ " om-owl:procedure ?procedure ; "
				+ " om-owl:result ?result ; "
				+ " om-owl:samplingTime ?samplingTime . "
				+ " ?result om-owl:floatValue ?value ; "
				+ " om-owl:uom ?uom . "
				+ " ?samplingTime owl-time:inXSDDateTime ?time " + "} ";
		final File folder = new File("/home/hadoop/rdf2/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/rdf2/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
			model.close();
		}// end of for (int i = 0; i < listOfFiles.length; i++)
		final Query query = QueryFactory.create(SELECTQuery);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();

		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
		qexec.close();
	}

	public static String runSelectTimeQuery(final Model model,
			final String observation) throws ParseException {
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ " SELECT ?d "
				+ " WHERE { sens-obs:"
				+ observation
				+ " om-owl:samplingTime ?samplingTime . "
				+ " ?samplingTime owl-time:inXSDDateTime ?d  " + "} ";
		String date = null;
		final Query query = QueryFactory.create(queryString);
		// QueryExecution qexec = QueryExecutionFactory.create(query,
		// ReadModel);
		final QueryExecution qexec = QueryExecutionFactory.create(query, model);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			final QuerySolution qs = result.nextSolution();
			// date = qs.getLiteral("d").getString();
			final String timesplit2[] = qs.getLiteral("d").getString()
					.split("\\^");
			date = timesplit2[0];
			// date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss",
			// Locale.ENGLISH).parse(timesplit2[0]);
		}
		return date;
	}

	public static void runSelectPredicate() throws ParseException,
	FileNotFoundException {
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/value-observations.csv");
		final Model models = ModelFactory.createDefaultModel();

		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?s " + " WHERE { ?s lld:hasResult ?o} ";
		String inputFileName = "";
		final File folder = new File(
				"/home/hadoop/ISWC-2016/690-reduced-TV-value-molecule/");
		final File[] listOfFiles = folder.listFiles();
		final boolean found = false;

		for (int i = 0; i < listOfFiles.length; i++) {
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/690-reduced-TV-value-molecule/"
					+ inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			final Model model = ModelFactory.createDefaultModel();

			model.read(in, null, "N3");
			models.add(model);
		}

		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}

	}

	public static void runSelectSchemaQuery(final Model model,
			final String observation) {
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ " SELECT ?type ?property ?procedure "
				+ " WHERE { sens-obs:"
				+ observation
				+ " a ?type ; "
				+ " om-owl:observedProperty ?property ; "
				+ " om-owl:procedure ?procedure ; " + "} ";
		final Query query = QueryFactory.create(queryString);
		// QueryExecution qexec = QueryExecutionFactory.create(query,
		// ReadModel);
		final QueryExecution qexec = QueryExecutionFactory.create(query, model);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			final QuerySolution qs = result.nextSolution();
			type = qs.getResource("type").getLocalName();
			property = qs.getResource("property").getLocalName();
			procedure = qs.getResource("procedure").getLocalName();
		}
		// System.out.println("type=" + type + "\nProperty=" + property +
		// "\nProcedure=" + procedure + "\nUOM=" + uom);
	}

	public static Model runConstructQuery(final Model model,
			final List<String> observation) {
		String queryString;
		final Model result = ModelFactory.createDefaultModel();
		for (int i = 0; i < observation.size(); i++) {
			System.out.println("constructing time and measure links " + i);
			queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
					+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
					+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
					+ "prefix owl-time: <http://www.w3.org/2006/time#> "
					+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
					+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
					+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
					+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
					+ "prefix lld: <http://linkeddata.com/ontology#> "
					+ " CONSTRUCT { sens-obs:"
					+ observation.get(i)
					+ " om-owl:result ?measurement ; "
					+ " om-owl:samplingTime ?samplingTime "
					+ " } "
					+ " WHERE { sens-obs:"
					+ observation.get(i)
					+ " om-owl:result ?measurement ; "
					+ " om-owl:samplingTime ?samplingTime  " + " } ";
			final Query query = QueryFactory.create(queryString);
			// QueryExecution qexec = QueryExecutionFactory.create(query,
			// ReadModel);
			final QueryExecution qexec = QueryExecutionFactory.create(query,
					model);
			result.add(qexec.execConstruct());
		}
		// ResultModel.add(result);
		return result;
	}

	public static Model ConstructEquidistantData(final Model model,
			final String observation, final Float value) throws ParseException {

		final String resultBN = mapBlankNode.get(value);
		final Model result = ModelFactory.createDefaultModel();
		final Calendar calStart = Calendar.getInstance();
		final Calendar calEnd = Calendar.getInstance();
		// calStart.setTime(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss",
		// Locale.ENGLISH).parse(dStart)
		result.setNsPrefix("om-owl",
				"http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");
		result.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		result.setNsPrefix("sens-obs", "http://knoesis.wright.edu/ssw/");
		result.setNsPrefix("owl-time", "http://www.w3.org/2006/time#");
		result.setNsPrefix("owl", "http://www.w3.org/2002/07/owl#");
		result.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
		result.setNsPrefix("weather",
				"http://knoesis.wright.edu/ssw/ont/weather.owl#");
		result.setNsPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		result.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
		result.setNsPrefix("lld", "http://linkeddata.com/ontology#");
		result.add(
				result.createResource("_:b" + Count),
				result.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				result.createResource("http://knoesis.wright.edu/ssw/ont/weather.owl#"
						+ type));
		result.add(
				result.createResource("_:b" + Count),
				result.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#observedProperty"),
				result.createResource("http://knoesis.wright.edu/ssw/ont/weather.owl#"
						+ property));
		result.add(
				result.createResource("_:b" + Count),
				result.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#procedure"),
				result.createResource("http://knoesis.wright.edu/ssw/"
						+ procedure));
		result.add(result.createResource("_:b" + Count), result
				.createProperty("http://linkeddata.com/ontology#hasResult"),
				result.createResource(resultBN));
		result.add(
				result.createResource("_:b" + Count),
				result.createProperty("http://linkeddata.com/ontology#hasSamplingTime"),
				result.createResource("_:t" + Count));
		result.add(result.createResource("_:b" + Count), result
				.createProperty("http://linkeddata.com/ontology#hasOrder"),
				result.createResource("_:obOrderBN" + Count));
		result.add(
				result.createResource("_:t" + Count),
				result.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				result.createResource("http://www.w3.org/2006/time#Instant"));
		result.add(result.createResource("_:t" + Count), result
				.createProperty("http://linkeddata.com/ontology#timeDiff"),
				result.createTypedLiteral(timeDiff));
		result.add(result.createResource("_:t" + Count), result
				.createProperty("http://linkeddata.com/ontology#startTime"),
				result.createLiteral(dStart + "^^xsd:dateTime"));
		result.add(
				result.createResource("_:t" + Count),
				result.createProperty("http://linkeddata.com/ontology#endTime"),
				result.createLiteral(dEnd + "^^xsd:dateTime"));
		result.add(
				result.createResource("http://knoesis.wright.edu/ssw/"
						+ procedure),
						result.createProperty("http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#generatedObservation"),
						result.createResource("_:b" + Count));

		return result;
	}

	public static Model ConstructSensorData(final Model model) {
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " CONSTRUCT {  ?sensor om-owl:generatedObservation ?observation "
				+ " } "
				+ " WHERE { ?sensor om-owl:generatedObservation ?observation  } ";
		final Query query = QueryFactory.create(queryString);
		// QueryExecution qexec = QueryExecutionFactory.create(query,
		// ReadModel);
		final QueryExecution qexec = QueryExecutionFactory.create(query, model);
		Model result = ModelFactory.createDefaultModel();
		result = qexec.execConstruct();
		// ResultModel.add(result);
		return result;
	}

	public static Model addOriginalData(final Model model,
			final String observation, final float value)
					throws FileNotFoundException {

		final String resultBN = mapBlankNode.get(value);
		Model result = ModelFactory.createDefaultModel();

		// System.out.println("Blank node for original data for value " + value
		// + " is = " + resultBN);
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " CONSTRUCT { sens-obs:"
				+ observation
				+ " a ?type ; "
				+ " om-owl:observedProperty ?property ;"
				+ " om-owl:procedure ?procedure . "
				+
				// " lld:hasResult " + resultBN + ";" +
				// " om-owl:samplingTime ?samplingTime ." +
				" ?samplingTime a owl-time:Instant ; "
				+ " owl-time:inXSDDateTime ?d . "
				+ " ?procedure om-owl:generatedObservation sens-obs:"
				+ observation
				+ " } "
				+ " WHERE { sens-obs:"
				+ observation
				+ " a ?type ; "
				+ " om-owl:observedProperty ?property ; "
				+ " om-owl:procedure ?procedure . "
				+
				// " om-owl:result ?measurement ;" +
				// " om-owl:samplingTime ?samplingTime ." +
				// " ?measurement a om-owl:MeasureData ;" +
				// " om-owl:floatValue ?v ; " +
				// " om-owl:uom ?uom ." +
				" ?samplingTime a owl-time:Instant ; "
				+ " owl-time:inXSDDateTime ?d . "
				+ " ?procedure om-owl:generatedObservation sens-obs:"
				+ observation + " } ";
		// " BINDINGS ?ob { (sens-obs:" + obList.get(i) + " ) }";
		// bindings + " }";
		final Query query = QueryFactory.create(queryString);
		// QueryExecution qexec = QueryExecutionFactory.create(query,
		// ReadModel);
		final QueryExecution qexec = QueryExecutionFactory.create(query, model);
		result = qexec.execConstruct();
		result.add(result.createResource("http://knoesis.wright.edu/ssw/"
				+ observation), result
				.createProperty("http://linkeddata.com/ontology#hasResult"),
				result.createResource(resultBN));

		return result;
	}

	public static void getDistinctPhenomenon() {
		final List<String> pList = new ArrayList<String>();
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ " SELECT DISTINCT ?pType "
				+ " WHERE { ?ob  a ?pType ; "
				+ " om-owl:observedProperty ?property " + "} ";
		String inputFileName = "";
		final File folder = new File("/home/hadoop/ISWC-2016/rdf/");
		final File[] listOfFiles = folder.listFiles();
		boolean found = false;
		QuerySolution qs = null;
		for (int i = 0; i < listOfFiles.length; i++) {
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/rdf/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			final Model model = ModelFactory.createDefaultModel();
			model.read(in, null, "N3");
			final Query query = QueryFactory.create(queryString);
			final QueryExecution qexec = QueryExecutionFactory.create(query,
					model);
			final ResultSet result = qexec.execSelect();
			while (result.hasNext()) {
				qs = result.nextSolution();
				for (int j = 0; j < pList.size(); j++) {
					if (pList.get(j).equals(
							qs.getResource("pType").getLocalName())) {
						found = true;
						break;
					}// end of
				}// end of for(int j=0;j<pList.size();j++)
				if (found) {
					found = false;
				}// end of if(found)
				else {
					pList.add(qs.getResource("pType").getLocalName());
				}
			} // end while (results.hasNext())
		}// end of for (int i = 0; i < listOfFiles.length; i++)
		for (int j = 0; j < pList.size(); j++) {
			System.out.println(pList.get(j));
		}
	}

	public static void selecthasTimeTriple() throws FileNotFoundException {
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/ISWC-2016/time.csv");
		final List<String> pList = new ArrayList<String>();
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?o (GROUP_CONCAT(?s) as ?observations) "
				+ " WHERE { ?s lld:hasTime ?o  " + "} GROUP BY ?o ";
		String inputFileName = "";
		final File folder = new File("/home/hadoop/ISWC-2016/45001-reduced/");
		final File[] listOfFiles = folder.listFiles();
		final boolean found = false;

		for (int i = 0; i < listOfFiles.length; i++) {
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/45001-reduced/"
					+ inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			final Model model = ModelFactory.createDefaultModel();

			model.read(in, null, "N3");
			final Query query = QueryFactory.create(queryString);
			final QueryExecution qexec = QueryExecutionFactory.create(query,
					model);
			final ResultSet result = qexec.execSelect();
			if (result.hasNext()) {
				ResultSetFormatter.outputAsCSV(out, result);
			}
		}
	}

	public static void retrieveRDFofOnePhenomena() throws FileNotFoundException {
		String inputFileName = "";
		String inputFileName1 = "";

		final File folder = new File("/home/hadoop/April2003-Temp/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();

		for (int i = 0; i < listOfFiles.length; i++) {

			Model results = ModelFactory.createDefaultModel();
			results.setNsPrefix("om-owl",
					"http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");
			results.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			results.setNsPrefix("sens-obs", "http://knoesis.wright.edu/ssw/");
			results.setNsPrefix("owl-time", "http://www.w3.org/2006/time#");
			results.setNsPrefix("owl", "http://www.w3.org/2002/07/owl#");
			results.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
			results.setNsPrefix("weather",
					"http://knoesis.wright.edu/ssw/ont/weather.owl#");
			results.setNsPrefix("rdf",
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			results.setNsPrefix("om-owl",
					"http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");
			results.setNsPrefix("om-owl",
					"http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");

			final Model model = ModelFactory.createDefaultModel();
			inputFileName1 = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/April2003-Temp/" + inputFileName1;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			final OutputStream out = new FileOutputStream(
					"/home/hadoop/April2003-Temp-Measurements/"
							+ inputFileName1);
			// final String queryString =
			// "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
			// + "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
			// + "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
			// + "prefix owl-time: <http://www.w3.org/2006/time#> "
			// + "prefix owl: <http://www.w3.org/2002/07/owl#> "
			// + "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
			// +
			// "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
			// + "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
			// + "prefix lld: <http://linkeddata.com/ontology#> "
			// + " CONSTRUCT { ?ob a weather:TemperatureObservation ; "
			// + " om-owl:observedProperty ?property ;"
			// + " om-owl:procedure ?procedure ;"
			// + " om-owl:result ?measurement ;"
			// + " om-owl:samplingTime ?samplingTime ."
			// + " ?samplingTime a owl-time:Instant ; "
			// + " owl-time:inXSDDateTime ?d . "
			// + " ?procedure om-owl:generatedObservation ?ob . "
			// + " ?measurement a om-owl:MeasureData ; "
			// + " om-owl:floatValue  ?v ; "
			// + " om-owl:uom ?uom ."
			// + " } "
			// + " WHERE { ?ob a weather:TemperatureObservation ; "
			// + " om-owl:observedProperty ?property ;"
			// + " om-owl:procedure ?procedure ;"
			// + " om-owl:result ?measurement ;"
			// + " om-owl:samplingTime ?samplingTime ."
			// + " ?samplingTime a owl-time:Instant ; "
			// + " owl-time:inXSDDateTime ?d . "
			// + " ?procedure om-owl:generatedObservation ?ob . "
			// + " ?measurement a om-owl:MeasureData ; "
			// + " om-owl:floatValue  ?v ; " + " om-owl:uom ?uom ." + "} ";
			final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
					+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
					+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
					+ "prefix owl-time: <http://www.w3.org/2006/time#> "
					+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
					+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
					+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
					+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
					+ "prefix lld: <http://linkeddata.com/ontology#> "
					+ " CONSTRUCT { ?ob om-owl:result ?m ."
					+ " ?m om-owl:floatValue ?v  "
					+ " } "
					+ " WHERE { ?ob om-owl:result ?m ."
					+ " ?m om-owl:floatValue ?v } ";
			final Query query = QueryFactory.create(queryString);
			final QueryExecution qexec = QueryExecutionFactory.create(query,
					model);
			results = qexec.execConstruct();
			results.write(out, "N3");
		}
	}

	public static void getAllOriginalValues() throws FileNotFoundException {
		String queryString = "";
		String inputFileName = "";
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/ISWC-2016/originalValues.csv");
		queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				// + " SELECT (AVG(?value) as ?average) "
				+ " SELECT ?value "
				+ " WHERE { ?observation om-owl:result ?measurement ."
				+ " ?measurement om-owl:floatValue ?value . } ";

		final File folder = new File("/home/hadoop/ISWC-2016/690/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();
		final Model results = ModelFactory.createDefaultModel();

		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/690/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
		}// end of for (int i = 0; i < listOfFiles.length; i++)

		Query query;
		QueryExecution qexec;
		ResultSet result;
		query = QueryFactory.create(queryString);
		qexec = QueryExecutionFactory.create(query, models);
		result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
	}

	public static void getAllReducedValues() throws FileNotFoundException {
		String queryString = "";
		String inputFileName = "";
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/ISWC-2016/reducedValues.csv");
		queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ " PREFIX lld: <http://linkeddata.com/ontology#> "
				// + " SELECT (AVG(?value) as ?average) "
				+ " SELECT ?value "
				+ " WHERE { {?observation lld:hasResult ?measurement ."
				+ "FILTER (!regex(str(?observation),'http://linkeddata.com/ontology#','i')) "
				+ " ?measurement om-owl:floatValue ?value . }"
				+ " UNION "
				+ "{?observationBN lld:hasResult ?measurement ;"
				+ " lld:hasOrder ?obsorder . "
				+ "?obsorder ?p ?obselement . "
				+ "FILTER ( ?p != rdf:type )  "
				+ " ?measurement om-owl:floatValue ?value .}"

				+ "} ";

		final File folder = new File("/home/hadoop/ISWC-2016/690-reduced/");
		final File[] listOfFiles = folder.listFiles();
		final Model models = ModelFactory.createDefaultModel();
		final Model results = ModelFactory.createDefaultModel();

		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/690-reduced/"
					+ inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
		}// end of for (int i = 0; i < listOfFiles.length; i++)

		Query query;
		QueryExecution qexec;
		ResultSet result;
		query = QueryFactory.create(queryString);
		qexec = QueryExecutionFactory.create(query, models);
		result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
	}

	public static void concatenateRDFFiles(final String path_to_original_data,
			final String path_to_reduced_data, final String file_name)
					throws FileNotFoundException {
		String inputFileName = "";
		final OutputStream out = new FileOutputStream(path_to_reduced_data
				+ file_name);
		final File folder = new File(path_to_original_data);
		final File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			inputFileName = listOfFiles[i].getName();
			inputFileName = path_to_original_data + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			final Model model = ModelFactory.createDefaultModel();
			model.read(in, null, "N3");
			// ReadModel.read(in, null, "N3");
			ReadModel.add(model);
			// model.add(model);
			model.close();
		}
		ReadModel.write(out, "N-TRIPLES");
		ReadModel.close();
	}// end of combineFiles()

	public static void removeNewlines() throws IOException {

		final FileReader fr = new FileReader(
				"C:/Users/karim/Documents/Projects/Sensor-Data-Storage/Dataset/rdf-reduced/test/690_2009_9_18.n3");
		final BufferedReader br = new BufferedReader(fr);
		final FileWriter fw = new FileWriter(
				"C:/Users/karim/Documents/Projects/Sensor-Data-Storage/Dataset/rdf-reduced/test/690_2009_9_18_new.n3");
		String line;

		while ((line = br.readLine()) != null) {
			// line = line.trim(); // remove leading and trailing whitespace
			if (!line.equals("")) // don't write out blank lines
			{
				fw.write(line, 0, line.length());
			}
		}
		fr.close();
		fw.close();

	} // end of removeNewlines()

	public static void convertXMLtoNT() throws IOException {
		final Model model = ModelFactory.createDefaultModel();
		final Model models = ModelFactory.createDefaultModel();
		String inputFileName = "";
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/ISWC-2016/NTdata/original690.nt");
		final File folder = new File("/home/hadoop/ISWC-2016/testComplete/");
		final File[] listOfFiles = folder.listFiles();
		// final Model models = ModelFactory.createDefaultModel();
		// final Model results = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			// final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/ISWC-2016/testComplete/"
					+ inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
		}// end of for (int i = 0; i < listOfFiles.length; i++)
		models.write(out, "	N-Triples");
	} // end of convertXMLtoN3()

	public static void retrievTimeSortedObservations() throws IOException {
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?ob ?v ?property ?procedure (GROUP_CONCAT(?d) as ?date) "
				+ " WHERE { ?ob a weather:TemperatureObservation ;"
				+ " om-owl:procedure ?procedure ; "
				+ " om-owl:observedProperty ?property ; "
				+ " om-owl:result ?result ; "
				+ " om-owl:samplingTime ?samplingTime . "
				+ " ?result om-owl:floatValue ?v . "
				+ " ?samplingTime a owl-time:Instant ;"
				+ " owl-time:inXSDDateTime ?d "
				+ "} "
				+ " Group By ?ob ?v ?property ?procedure "
				+ "ORDER BY ?property ?procedure ASC(?date) ";

		final File folder = new File("/home/hadoop/rdf2/");
		final File[] listOfFiles = folder.listFiles();
		String inputFileName = "";
		final Model models = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/rdf2/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			in.close();
			models.add(model);
			model.close();
		}// end of for (int i = 0; i < listOfFiles.length; i++)

		System.out.println("Data Loaded.");
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/sortedTemperatureObs.csv");
		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
	}

	public static void retrievValueSortedObservations() throws IOException {
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix lld: <http://linkeddata.com/ontology#> "
				+ " SELECT ?ob ?property ?procedure ?value "
				+ " WHERE { ?ob a weather:TemperatureObservation ;"
				+ " om-owl:procedure ?procedure ; "
				+ " om-owl:observedProperty ?property ; "
				+ " om-owl:result ?result . "
				+ " ?result om-owl:floatValue ?value "
				+ "} "
				+ "ORDER BY ?procedure ?property ASC(?value) ";

		final File folder = new File("/home/hadoop/rdf2/");
		final File[] listOfFiles = folder.listFiles();
		String inputFileName = "";
		final Model models = ModelFactory.createDefaultModel();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFileName = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/rdf2/" + inputFileName;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			in.close();
			models.add(model);
			model.close();
		}// end of for (int i = 0; i < listOfFiles.length; i++)

		System.out.println("Data Loaded.");
		final OutputStream out = new FileOutputStream(
				"/home/hadoop/sortedTemperatureObs.csv");
		final Query query = QueryFactory.create(queryString);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final ResultSet result = qexec.execSelect();
		if (result.hasNext()) {
			ResultSetFormatter.outputAsCSV(out, result);
		}
	}

	public static void rdf2rdf(final String path_to_original_data,
			final String path_to_reduced_data) throws FileNotFoundException {
		String inputFile;
		String inputFileName;
		final File folder = new File(path_to_original_data);
		final File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			LongitudinalFactorization.reducedModel
			.setNsPrefix("om-owl",
					"http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#");
			model.setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			model.setNsPrefix("sens-obs", "http://knoesis.wright.edu/ssw/");
			model.setNsPrefix("owl-time", "http://www.w3.org/2006/time#");
			model.setNsPrefix("owl", "http://www.w3.org/2002/07/owl#");
			model.setNsPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
			model.setNsPrefix("weather",
					"http://knoesis.wright.edu/ssw/ont/weather.owl#");
			model.setNsPrefix("rdf",
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			model.setNsPrefix("lld", "http://linkeddata.com/ontology#");
			inputFile = listOfFiles[i].getName();
			inputFileName = path_to_original_data + inputFile;
			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N-TRIPLES");
			final String extensionRemoved = inputFile.split("\\.")[0];

			final OutputStream out = new FileOutputStream(path_to_reduced_data
					+ extensionRemoved + ".n3");
			model.write(out, "N3");

		}
	}

	public static void createMolecule() throws IOException {
		String inputFile = "";
		String inputFileName = "";
		final Model models = ModelFactory.createDefaultModel();
		final File folder = new File("/home/hadoop/example/observations/");
		final File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			final Model model = ModelFactory.createDefaultModel();
			inputFile = listOfFiles[i].getName();
			inputFileName = "/home/hadoop/example/observations/" + inputFile;

			final InputStream in = FileManager.get().open(inputFileName);
			if (in == null)
				throw new IllegalArgumentException("File: " + inputFileName
						+ " not found");
			System.out.println("File read.." + inputFileName);
			model.read(in, null, "N3");
			models.add(model);
			// model.close();
		}
		final String content = new String(Files.readAllBytes(Paths
				.get("/home/hadoop/example-query/q.sparql")));
		final String queryString = "prefix om-owl: <http://knoesis.wright.edu/ssw/ont/sensor-observation.owl#> "
				+ "prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
				+ "prefix sens-obs: <http://knoesis.wright.edu/ssw/> "
				+ "prefix owl-time: <http://www.w3.org/2006/time#> "
				+ "prefix owl: <http://www.w3.org/2002/07/owl#> "
				+ "prefix xsd: <http://www.w3.org/2001/XMLSchema#> "
				+ "prefix weather: <http://knoesis.wright.edu/ssw/ont/weather.owl#> "
				+ "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> "
				+ "prefix ssd: <http://linkeddata.com/ontology#> "
				+ "prefix fn:   <http://www.w3.org/2005/xpath-functions#>"
				+ " CONSTRUCT { ssd:m ssd:ObservationMolecule ?new . } "
				+ " WHERE {  "
				+ " SELECT DISTINCT ?mMolecule "// ( COUNT(?m) as ?count )
				+ " WHERE { "
				+ " ?m om-owl:uom ?u . "
				+ " ?m om-owl:floatValue ?v "
				+ " BIND(fn:replace(?u, \"e\", \"Z\") as ?new) "
				// + " BIND ( fn:replace (?u, \"e\", \"Z\" ) as ?o )  "
				// + " BIND ( CONCAT (?u, \"e\",\"-\") as ?vNew ) "
				+ "}  } ";
		// + " } } ";
		// " BIND ( fn:concat ( \"ssd:\", ?t, (fn:replace(?w, \"_\",\"-\") as ?wNew ) as ?oMolecule ) "
		// + " BIND ( fn:concat ( \"ssd:\", ?t , ?w ) as ?oMolecule ) "
		// + " } }" + " GROUP BY ?t ";

		final Query query = QueryFactory.create(content);
		final QueryExecution qexec = QueryExecutionFactory
				.create(query, models);
		final Model constructModel = qexec.execConstruct();
		System.out.println("Construct result = " + constructModel.toString());
		constructModel.write(System.out, "N3");
		// models.write(System.out, "N3");
		qexec.close();
		constructModel.close();
		models.close();
	}
} // end Class LongitudinalFactorization