# Dependencies

As the Microsoft driver library is not available in a public repo you need to add it to your local maven repo:

	mvn install:install-file -Dfile=externalLibraries/sqljdbc-4.0.2206.100.jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc -Dversion=4.0.2206.100 -Dpackaging=jar

# Build and run the application

## To build with Maven

    mvn package

To run the fat jar:

    java -jar target/mssqlbatchimporter-1.0.0.jar

The build.gradle uses the Gradle shadowJar plugin to assemble the application and all it's dependencies into a single "fat" jar.

## To build with Gradle

    ./gradlew shadowJar

To run the fat jar:

    java -jar build/libs/Importer-all.jar