## Building and Testing

The spark connector is built with Scala 2.10 and sbt 0.13.5 to 0.13.7.

### Building Artifacts

To build against Apache Geode, you need to build Geode first and publish the jars
to local repository. In the root of Geode directory, run:

```
./gradlew clean build -x test
```

The following jar files will be created:
 - `geode-spark-connector/build/libs/geode-spark-connector-1.0.0.jar`
 - `geode-functions/build/libs/geode-functions-1.0.0.jar`
 - `geode-spark-demos/build/libs/geode-spark-demos-1.0.0.jar `


Next: [Quick Start](2_quick.md)
