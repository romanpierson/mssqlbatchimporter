package com.mdac.tools.sql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 * 
 * Example Usage:
 * 
 * -test 
 * -d "D://workspaces//test"
 * -f "D://workspaces//dbo.products.Table.sql" 
 * -h localhost 
 * -s schema 
 * -u user
 * -p password
 * 
 * 
 * @author roman@mdac-consulting.com
 *
 */
public class MSSQLBatchImporter {

	
	private final Logger logger = Logger.getLogger("default");
	
	private boolean isTest = true;
	
	private File importFile;
	private File importDirectory;
	
	private DatabaseConnector databaseConnector;
	
	private final String lineSeparator = System.getProperty("line.separator");
	private final String tab ="\t";
	
	private static final String OPTION_TEST = "test";
	private static final String OPTION_FILE = "f";
	private static final String OPTION_DIRECTORY = "d";
	
	private static final String OPTION_DB_HOST = "h";
	private static final String OPTION_DB_PORT = "port";
	private static final String OPTION_DB_NAME = "s";
	private static final String OPTION_DB_USER = "u";
	private static final String OPTION_DB_PASSWORD = "p";
	private static final String OPTION_HELP = "help";
	
	public static void main(String[] args) throws IOException {
		
		final MSSQLBatchImporter importer = MSSQLBatchImporter.createInstance(args);
		
		if(importer != null && !importer.isTest){
			importer.performImport();
		}
		
	}
	
	
	
	// Avoid instantiation
	private MSSQLBatchImporter(){
		
	}
	
	
	private static void initializeLogger(final Logger logger){
		
		final SimpleLayout layout = new SimpleLayout(); 
		
		// Create the console logger for standard output
		final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
		
		logger.addAppender(consoleAppender);
		
	}

	public static MSSQLBatchImporter createInstance(final String[] options){
	
		final MSSQLBatchImporter importer = new MSSQLBatchImporter();
		
		initializeLogger(importer.logger);
		
		if(importer.isOptionSet(OPTION_HELP, options)){
			importer.displayHelp();
			return null;
		}
		
		importer.isTest = importer.isOptionSet(OPTION_TEST, options);
		
		final String directoryPath = importer.getOptionValue(OPTION_DIRECTORY, options);
		final String filePath = importer.getOptionValue(OPTION_FILE, options);
		
		if(directoryPath == null && filePath == null){
			importer.logger.error("You must specify either a file or directory!");
			importer.displayHelp();
			return null;
		}
		
		if(directoryPath != null){
			// Directory Mode
			final File directory = importer.getValidDirectory(directoryPath);
			
			if(directory == null){
				importer.logger.error("Directory [" + directoryPath + "] is invalid or has any files");
				importer.displayHelp();
				return null;
			}
			
			importer.importDirectory = directory;
			
		} else {
			// File Mode
			final File file = importer.getValidFile(filePath);
			
			if(file == null){
				importer.logger.error("File [" + filePath + "] is invalid");
				importer.displayHelp();
				return null;
			}
			
			importer.importFile = file;
		}
		
		// Database params
		final String host = importer.getOptionValue(OPTION_DB_HOST, options);
		final String port = importer.getOptionValue(OPTION_DB_PORT, options) == null ? "1433" : importer.getOptionValue(OPTION_DB_PORT, options);
		final String schema = importer.getOptionValue(OPTION_DB_NAME, options);
		final String user = importer.getOptionValue(OPTION_DB_USER, options);
		final String password = importer.getOptionValue(OPTION_DB_PASSWORD, options);
		
		if(host == null || port == null || schema == null || user == null || password == null){
			importer.logger.error("Invalid database options - please check");
			importer.displayHelp();
			return null;
		}
		
		// Test the connection
		importer.databaseConnector = new DatabaseConnector(host, port, schema, user, password, 4000, importer.logger);
		
		importer.logger.info("Testing the connection to database....");
		final boolean success = importer.databaseConnector.connect();
		
		if(!success){
			importer.logger.error("Error when testing database connection");
			importer.displayHelp();
			return null;
		}
		
		importer.databaseConnector.disconnect();
		
		return importer;
	}
	
	private File getValidDirectory(final String directoryPath){
	
		final File file = new File(directoryPath); 
		
		if(file != null && file.isDirectory() && file.listFiles() != null && file.listFiles().length > 0){
			return file;
		}
		
		return null;
		
	}
	
	private File getValidFile(final String filePath){
		
		final File file = new File(filePath); 
		
		if(file != null && file.isFile()){
			return file;
		}
		
		return null;
		
	}
	
	/**
	 * 
	 * Checks if a certain option is set
	 * 
	 * @param optionToCheck	The option to be verified
	 * @param options		The list of option values
	 * @return				If set or not
	 */
	private boolean isOptionSet(final String optionToCheck, final String[] options){
		
		final String completeOptionToCheck = "-" + optionToCheck;
		
		for(final String option : options){
			if(option.equals(completeOptionToCheck)){
				return true;
			}
		}
		
		return false;
	}
	
	private String getOptionValue(final String optionToCheck, final String[] options){
	
		final String completeOptionToCheck = "-" + optionToCheck;
		
		for(int i = 0; i < options.length; i++){
			if(options[i].equals(completeOptionToCheck) && options.length >= (i + 1)){
				return options[(i + 1)];
			}
		}
		
		return null;
		
	}
	
	public void displayHelp(){
		
		final StringBuilder sb = new StringBuilder();
		
		sb.append("Available options:").append(lineSeparator);
		sb.append(tab).append('-').append(OPTION_HELP).append(tab).append("Display this help").append(lineSeparator);
		
		sb.append(tab).append('-').append(OPTION_TEST).append(tab).append("If passed options will be validated but no import is performed").append(lineSeparator);
		
		sb.append(tab).append('-').append(OPTION_FILE).append(tab).append("File to be imported").append(lineSeparator);
		sb.append(tab).append('-').append(OPTION_DIRECTORY).append(tab).append("Directory from where to import - has priority over file option").append(lineSeparator);
		
		sb.append(tab).append('-').append(OPTION_DB_HOST).append(tab).append("Database Host").append(lineSeparator);
		sb.append(tab).append('-').append(OPTION_DB_PORT).append(tab).append("Database Port - Default is 1433").append(lineSeparator);
		sb.append(tab).append('-').append(OPTION_DB_NAME).append(tab).append("Database Name").append(lineSeparator);
		sb.append(tab).append('-').append(OPTION_DB_USER).append(tab).append("Database User").append(lineSeparator);
		sb.append(tab).append('-').append(OPTION_DB_PASSWORD).append(tab).append("Database Password").append(lineSeparator);
		
		logger.info(sb.toString());
		
	}

	public void performImport(){
		
		logger.info("Starting import....");
		
		long startTS = System.currentTimeMillis();
		
		this.databaseConnector.connect();
		
		if(this.importDirectory != null){
			// Directory
			for(final File f : this.importDirectory.listFiles()){
				importFile(f);
				this.databaseConnector.flushBatch();
			}
			
		} else {
			// File
			importFile(this.importFile);
			this.databaseConnector.flushBatch();
		}
		
		this.databaseConnector.disconnect();
		
		logger.info("Import finished and took [" + (System.currentTimeMillis() - startTS) + "] ms");
		
	}
	
	private void importFile(final File file){
		
		logger.info("Importing file [" + file.getName() + "]");
		long startTS = System.currentTimeMillis();
		
		BufferedReader in = null;

		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF16"));
		} catch (final Exception ex) {
			logger.error("Error when trying to start to read file [" + file.getName() + "]", ex);
			return;
		}
		
		long rawLines = 0;
		long logicalLines = 0;
		String line = null;

		try {
			
			StringBuilder sbLine = new StringBuilder();
			boolean lineStarted = false;
			
			while ((line = in.readLine()) != null) {

				rawLines++;
				
				if(representsNewLine(line)){
					
					if(lineStarted){
						// We need to execute the pending line
						this.databaseConnector.executeUpdate(sbLine.toString());
						logicalLines++;
					}
					
					sbLine = new StringBuilder(line);
					lineStarted = true;
					
				} else {
					
					if (ignoreLine(line)) {
						
						if(lineStarted){
							// We need to execute the pending line
							this.databaseConnector.executeUpdate(sbLine.toString());
							lineStarted = false;
							logicalLines++;
						}
						
						continue;
					} 
					else if(lineStarted){
						
						sbLine.append(lineSeparator);
						sbLine.append(line);
						
					}
					
				}
				
			}
			
			// Last potential line needs to be flushed
			if(lineStarted){
				// We need to execute the pending line
				this.databaseConnector.executeUpdate(sbLine.toString());
				logicalLines++;
			}
			
		} catch (final IOException ioex) {
			logger.error("Error when trying to read file [" + file.getName() + "]", ioex);
		}

		try {
			in.close();
		} catch (IOException ex) {
			logger.error("Error when trying to close file [" + file.getName() + "]", ex);
		}

		logger.info("Finished to import file [" + file.getName() + "] with [" + rawLines + "/" + logicalLines +  "] lines took [" + (System.currentTimeMillis() - startTS) + "] ms");
		
	}
	
	protected boolean ignoreLine(final String line) {

		if (line == null || line.startsWith("USE") || line.trim().equals("GO")) {
			return true;
		}

		return false;
	}
	
	protected boolean emptyLine(final String line) {

		if (line != null && line.trim().equals("")) {
			return true;
		}

		return false;
	}
	
	protected boolean representsNewLine(final String line) {

		if (line != null && (line.startsWith("INSERT") || line.startsWith("UPDATE"))) {
			return true;
		}

		return false;
	}
	
}
