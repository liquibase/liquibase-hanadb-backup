# SAP HANA Liquibase Extension
A Liquibase extension adding support for SAP HANA

# Description
[Liquibase](https://www.liquibase.org/) has the concept of [Extensions](https://www.liquibase.org/extensions/index.html) to abstract from the differences between different database systems. This project implements such a Liquibase extension for SAP HANA. 

Include this in your application project to run Liquibase database migration scripts against a SAP HANA database instance.

# Requirements
- [Java 8](http://www.sapmachine.io) or higher
- Apache [Maven](https://maven.apache.org/)
- A [SAP HANA](https://developers.sap.com/topics/sap-hana-express.html) database instance
- A [Git](https://git-scm.com/downloads) client

# Download and Installation

## Download
Clone or download the repository to your local computer. 

```
git clone https://github.com/SAP/hana-liquibase.git
cd hana-liquibase
```

## Installation
Use Maven to build and install the project to your local repository

```
mvn clean install
```

Running the tests requires a configured SAP HANA instance. To build the project without running the tests use the `-DskipTests` option

```
mvn clean install -DskipTests
```

# Configuration

## Database Access for Testing

Open the `src/test/resources/tests.properties` file.

```
url: jdbc:sap://hxehost:39015/
user: LIQUIBASE_TEST
password: L1qu1base_test
logLevel: DEBUG
```

Set up your database connection data and credentials.

**Example**

```
url: jdbc:sap://localhost:39015/
user: MY_DATABASE_USER
password: My_Pa$$word
logLevel: DEBUG
```

Save the file.

Build the project and run the tests:

```
mvn clean install
```

## Usage in Application Projects

If you want to use the Liquibase HANA extension in an application project, add the following to your build descriptor:

### Maven

```xml
<dependency>
    <groupId>com.sap.foss.hana</groupId>
    <artifactId>liquibase-hana</artifactId>
    <version>1.0.1</version>
</dependency>
```

### Gradle
```
implementation 'com.sap.foss.hana:liquibase-hana:1.0.1'
```

# How to obtain support

If you have questions or find a bug, please open an issue in this project's bug tracker.

# License

Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.

This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the [LICENSE](https://github.com/SAP/hana-liquibase/blob/master/LICENSE) file.
