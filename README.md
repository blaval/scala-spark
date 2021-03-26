# Scala-Spark

This project is a reference for learning Spark with Scala

## Running Tests

You will need to have maven, JDK and Scala installed.

```
mvn clean compile test
```

## Producing test coverage report

```
mvn clean scoverage:report
```

## Checking if coverage reaches level set in pom.xml

```
mvn clean scoverage:check
```