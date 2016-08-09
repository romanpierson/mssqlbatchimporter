package com.mdac.tools.sql;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.microsoft.sqlserver.jdbc.SQLServerException;

/**
 * 
 * 
 * @author roman@mdac-consulting.com
 *
 */
public class DatabaseConnector{
	
	private Connection connection;
	private Statement statement;
	
	private final String host;
	private final String port;
	private final String schema;
	private final String user;
	private final String password;
	
	private final int batchSize;
	private int batchCounter = 0;
	private final Logger logger;
	
	public DatabaseConnector(final String host, final String port, final String schema, final String user, String password, final int batchSize, final Logger logger){
		
		this.host = host;
		this.port = port;
		this.schema = schema;
		this.user = user;
		this.password = password;
		this.batchSize = batchSize;
		this.logger = logger;
		
	}
	
	/**
	 * 
	 * Connects to the given database
	 * 	
	 * @return	Successful or not
	 */
	public boolean connect(){
	
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			String url = "jdbc:sqlserver://" + this.host + ":" + this.port + ";databaseName=" + this.schema;
			connection = DriverManager.getConnection(url, this.user, this.password);
			
			statement = this.connection.createStatement();
			
		} catch (final Exception ex) {
			logger.error("Error when connecting to database", ex);
			return false;
		}
		
		logger.info("Successfully connected to database");
		return true;
	}
	
	public boolean disconnect(){
		
		if (this.statement != null) {
			try {
				this.statement.close();
			} catch (final SQLException sqlex) {
				logger.error("Error when closing statement", sqlex);
			}
		}
		if (this.connection != null) {
			try {
				this.connection.close();
			} catch (final SQLException sqlex) {
				logger.error("Error when closing connection", sqlex);
			}
		}
		
		logger.info("Disconnected from Database");
		return true;
	}
	
	public void flushBatch(){
	
		if(this.batchCounter == 0){
			return;
		}
		
		try {
			
			this.statement.executeBatch();
			this.statement.clearBatch();
			this.batchCounter = 0;
		}
		catch(final BatchUpdateException ssex){
				
			if(!ignoreInsertException(ssex.getMessage())){
				logger.error("Error when flushing batch", ssex);;
			}else{
				//
			}
				
		} catch (SQLException e) {
			logger.error("Error when flushing batch", e);
		}
		
		
	}
	
	public int executeUpdate(final String sqlLine){
			
		try {
			
			if(this.batchSize == 0){
			
				this.statement.execute(sqlLine);
				
			} else {
			
				this.statement.addBatch(sqlLine);
				this.batchCounter++;
				
				if(batchCounter > batchSize){
					
					this.statement.executeBatch();
					this.statement.clearBatch();
					this.batchCounter = 0;
				}
			}
		}
		catch(final BatchUpdateException ssex){
		
			if(!ignoreInsertException(ssex.getMessage())){
				logger.error("Error with line |"+ sqlLine + "|", ssex);
			} else{
				logger.error("Error with line |"+ sqlLine + "|", ssex);
			}
			
		}
		catch(final SQLServerException ssex){
			
			if(!ignoreInsertException(ssex.getMessage())){
				logger.error("Error with line |"+ sqlLine + "|", ssex);
			} else {
				logger.error("Error with line |"+ sqlLine + "|", ssex);
			}
			
			// TODO here we could log the error lines
			
		} catch (SQLException e) {
			logger.error("Error with line |"+ sqlLine + "|", e);
		}
		
		return 0;
		
	}
	
	protected boolean ignoreInsertException(final String message){
		
		if(message.contains("Violation of PRIMARY KEY")){
			return true;
		}
		
		return false;
		
	}

	
	
	
}
