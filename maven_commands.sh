# Installing depedencies
# Also compile to jar
mvn package

# Clean target/ + Build + execute
mvn clean install exec:java

# Copy depencies to target/dependency
mvn clean dependency:copy-dependencies

# Run jar (with Manifest included)
java -jar /recsys/prototype/graph_x_intro/target/my_app-1.0.jar