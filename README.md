# Loader for replicating data from Oracle into Impala

The aim of this integration is to capture transactions from Oracle databases (source) and to apply them into Impala database (target). The idea is to replicate the source database in almost real time or in a very short period of time.

On the Oracle side, Oracle Golden Gate (OGG) should be used with the Flat Files Adapter. This adapter should be configured to generate DSV files which contain the transactional data.

## How it works

When starting, the loader creates the final table (if it does not exist) using the configuration.

The loader checks periodically if there is new data to be transferred. If so, the loader follows several steps:

1. Copies new data to HDFS (without checking the contained data).
2. Creates a temporal (external) table in Impala with the new data located in HDFS.
3. Inserts all data from temporal table into final Impala table.
4. Deletes temporal table.
5. Deletes new local data.

## Create Eclipse project

You may want to continue with the development of this project and you want to use Eclipse (https://eclipse.org/) as development environment. If so, you could use Maven (https://maven.apache.org/) in order to generate the needed files for Eclipse.

As requirement you need Maven installed on your machine and run the command below when you are in the project folder.

```
mvn eclipse:eclipse
```

## Build project

The binaries are not shipped with the project, so they need to be generated.

This project depends of others such as Hadoop and Hive. We recommend the version of these dependencies are the same version that the target cluster has. The versions are configured in pom.xml file included in the project, you will find at the beginning two parameters that can be used for this purpose.

```
<hive.version>0.13.1-cdh5.3.3</hive.version>
<hadoop.version>2.5.0-cdh5.3.3</hadoop.version>
```

In the Cloudera repository you can look at the different versions that can be configured.
For Hadoop: https://repository.cloudera.com/cloudera/cloudera-repos/org/apache/hadoop/hadoop-client/
For Hive: https://repository.cloudera.com/cloudera/cloudera-repos/org/apache/hive/hive-common/

Once you have properly configured the versions, the binaries can be generated with the following command. 

```
mvn package
```

Binaries can be found at "target" folder. Two binaries are generated: one contains all the dependencies of the project (ogg-impala-*-jar-with-dependencies.jar), and the other one (ogg-impala*.jar) only contains the developed code. If you use the jar with dependencies, you need to neither deploy any libraries nor add them to the class path.  

## Golden Gate configuration

An official user guide can be found in this link: https://docs.oracle.com/goldengate/gg12121/gg-adapter/GADAD/flatfile_config.htm

With the purpose that Impala can read data files generated by Flat Files Adapter, it should be configured with the following parameters:

  * Remove colon between date and time in time stamps (goldengate.userexit.datetime.removecolon=true)
  * Delimited separated files (writer.mode=DSV)
  * Should not contain meta data (writer.includebefores=false, writer.includecolnames=false, writer.omitplaceholders=true)
  * Should contain all values (writer.diffsonly=false, writer.omitvalues=false)
  * Text as ASCII (writer.rawchars=false)
  * Without quotes (writer.dsv.quotes.policy=none)
  * Separated data files per table (writer.files.onepertable=true)
  * Null values should be represented by an empty string (writer.dsv.nullindicator.chars=)
  * Default Impala delimiter expressed as hexadecimal (writer.dsv.fielddelim.code=01) 
  * Without escape character (writer.dsv.fielddelim.escaped.chars=)
  
If we use the path B for configuring the loader, these parameters could be different since it is the user who specifies the format of the input files in the queries.

## Loader configuration

In order to the loader is able to copy new data into HDFS, it should be configured using a separated properties file (otherwise default values are used such as host=localhost). This file must be in the class path, be named "core-site.xml" and contain as minimum:

```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<configuration>
	<property>
		<name>fs.defaultFS</name>
		<value>hdfs://<HOST>:<PORT>/</value>
	</property>
</configuration>
```

There is another properties file that must be created and pointed out by the first argument when running the loader. This file should contain parameter=value lines.

There are two ways to configure the loader. If we want the loader generates automatically the Impala queries that will be used, we must follow the configuration shown in path A. Otherwise, we must specify each query in the configuration as shown in path B.

Some parameters are common to both paths:

  * ogg.data.folders: input data folders paths separated by commas (mandatory).
  * ogg.control.file.name: control file name. This file is generated in each data folder by OGG (default with original table values: SCHEMA.TABLEcontrol).

  * batch.between.sec: in seconds the period of time for checking for new data (default: 30).
  * loader.failure.wait: in case of failure the period of time (in seconds) for trying again (default: 60).
  
  * impala.host: Impala host where queries will be run (default: localhost).
  * impala.port: Impala daemon HiveServer2 port (default: 21050)
  * impala.staging.table.directory: path into HDFS where new data will be stored temporally (default: ogg/staging/). NOTE: IF THIS DIRECTORY EXISTS, IT WILL BE DELETED WHEN STARTING THE LOADER.

A parameter determines which configuration path is used. If we specify the parameter below, path A is supposed to be used, otherwise path B is used.

  * ogg.definition.file.name: path to definition file (it must be created when configuring Flat Files Adapter).
  
### Path A) Infer queries

Each parameter that should be configured is shown below.
  
  * impala.table.schema: new final table schema (default: original Oracle schema)
  * impala.table.name: new final table name (default: original Oracle name)
  * impala.staging.table.schema: new temporally table schema (default: original Oracle schema)
  * impala.staging.table.name: new temporally table name (default: original Oracle name + "_staging")
  
Nevertheless, you could set one or more queries using parameters in path B.

REMINDER: If final table already exists, it will not be created so no modifications will be applied (parameters related with final table are applied only when creating final table).

#### Column customizations

The inferred information related with the column can be modified. You need to specify only the parameters related with the information that you want to customize. This customization will be applied in the final Impala table.

  * impala.table.columns.customize: original column name of the columns that you want to customize (below parameters: COLUMN_NAME).
  
Per specified column in the above parameter, we can set the following parameters.  
  
  * impala.table.column.COLUMN_NAME.name: new column name (default: original column name)
  * impala.table.column.COLUMN_NAME.datatype: new Impala data type (default: corresponding Impala data type)
  * impala.table.column.COLUMN_NAME.expression: Impala expression to be used for generating final value. It should return the final data type. as string (default: cast(COLUMN_NAME as CORRESPONDING_DATA_TYPE)).
  
NOTE: Columns of temporal table are created with original (as source Oracle table) columns names and all of them are STRING data type. Expressions are used in the query that query temporal table and insert this data into the final table which will have the specified data type.

If you set a new data type, a new expression that generates this new data type should be configured, otherwise import query may fail.
  
##### New columns

New columns can be added to the final Impala table using the same parameter for customized columns. A not used name should be used and all the parameters regarding the new columns must be configured (name, data type and expression).

The expression must use original column names of other columns.

##### Partitioning columns

In a similar way that we do with customized columns

  * impala.table.partitioning.columns: names separated by commas of partitioning columns
  * impala.table.partitioning.column.PART_COLUMN_NAME.datatype: Impala data type
  * impala.table.partitioning.column.PART_COLUMN_NAME.expression: Impala expression which generate the value of the partitioning column (should return the specified data type).

### Path B) Setting queries

Since we do not need to specify either table schema (impala.table.schema) or name (impala.table.name) because they are in the queries, the control file name must be given (in path A, control file name default value was inferred from table schema and name which came from definition file).

  * ogg.control.file.name: control file name (mandatory).

Instead of inferring all the queries, we can configure them directly with the following parameters. All of them are mandatory.

  * impala.staging.table.query.create: query for creating temporal (external) Impala table. This table must be able to read the generated files generated by Flat Files Adapter (the HDFS directory must be the one specified in impala.staging.table.directory).
  * impala.staging.table.query.drop: query for dropping temporally table.
  * impala.table.query.create: query for creating final Impala table (used if final table does not exist).
  * impala.table.query.insert: query for importing data form temporally table into final table.
  
Following parameters will not take effect.

  * impala.table.schema
  * impala.table.name
  * impala.staging.table.schema
  * impala.staging.table.name
  * impala.table.columns.customize and related
  * impala.table.partitioning.columns and related

## Running it!

The loader has been implemented in Java, so we need to run it using the JVM. A parameters file should be created and point out as first argument (if not specified, it will try to find ./config.properties). 

```
java -cp ogg-impala-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
   ch.cern.impala.ogg.datapump.ImpalaDataLoader \
   (path_to_parameters_file)
```







