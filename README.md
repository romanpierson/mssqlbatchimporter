# Dependencies

As the Microsoft driver library is not available in a public repo you need to add it to your local maven repo:

	mvn install:install-file -Dfile=externalLibraries/sqljdbc-4.0.2206.100.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc -Dversion=4.0.2206.100 -Dpackaging=jar

# Build and run the application

## To build with Maven

    mvn package

To run the fat jar:

    java -jar target/mssqlbatchimporter-0.9.0.jar

The build.gradle uses the Gradle shadowJar plugin to assemble the application and all it's dependencies into a single "fat" jar.

## To build with Gradle

    ./gradlew shadowJar

To run the fat jar:

    java -jar build/libs/mssqlbatchimporter-0.9.0.jar
    
## Usage Examples    

# Directory

	java -jar mssqlbatchimporter-0.9.0.jar -d "/users/data" -h localhost -u root -p 1234 -s schema1
	
# File

	java -jar mssqlbatchimporter-0.9.0.jar -f "/users/data/users.sql" -h localhost -u root -p 1234 -s schema1
 