
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

//	public static HashMap<Resource, Integer> mapMeasurementCounterCytoscape = new HashMap<Resource, Integer>();
	// public static HashMap<Resource, Integer> mapObservationCounterCytoscape =
	// new HashMap<Resource, Integer>();
//	public static HashMap<Resource, Integer> mapTruthCounterCytoscape = new HashMap<Resource, Integer>();
//	final static List<LineCSVCytoscape> lines = new ArrayList<LineCSVCytoscape>();
//	final static List<LineCSVCytoscape> linesFactorized = new ArrayList<LineCSVCytoscape>();

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
		 final String path_to_original_data = args[0];
		final String path_to_reduced_data = args[1];

		LongitudinalLinkedDataInMemoryUpdatedV10.singleFileFactorization(
						path_to_original_data,
				path_to_reduced_data);
		// generateOriginalCytoscapeCSV("/home/hadoop/Cytoscape_v3.4.0/RDFData/RDFFullFILES/RDFCSV/rdfOriginalcsvData.csv");
		// generateFactorizedCytoscapeCSV("/home/hadoop/Cytoscape_v3.4.0/RDFData/RDFFullFILES/RDFCSV/rdfFactorizedcsvData.csv");
		// countTriples("/home/hadoop/rdf2/", "/home/hadoop/streamRDF2/");
		// generateCSV();
		final long endTime = System.currentTimeMillis();
		final long totalTime = endTime - startTime;
		System.out.println("totalTime........." + totalTime);
	}

} // end Class LongitudinalFactorization