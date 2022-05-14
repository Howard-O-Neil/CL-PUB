# Installing depedencies
# Also compile to jar
# Work for both java + scala
mvn package

# Clean target/ + Build + execute
mvn clean install exec:java

# Copy dependencies to target/dependency
mvn clean dependency:copy-dependencies

# Pack all dependencies + source code into 1 jar
# Not working for scala
mvn assembly:single

# Run jar (with Manifest included)
java -jar /recsys/prototype/graph_x_intro/target/my_app-1.0.jar

# Run scala
mvn scala:run