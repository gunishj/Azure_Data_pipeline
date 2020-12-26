Data pipeline with azure Data Factory, Logic App and Hdinsight for sample covid cases schema

# Mastering the art of creating Automated pipelines using Azure

Hi All, Good morning ladies and gentleman though this start is little bit cliché but who cares just pass through this line I have a lot more interesting stuff beneath. So guys One thing almost everyone learnt in this lockdown is Learnt to make delicious food, yum!! and learnt one of the cloud service. If you haven’t learnt these two already no worries here I am to serve you my majesty.

Today we will make a delicious yummy full-fledged ETL pipeline, which works exactly same as our food pipe but it’s just for your day to day data.

To make our ETL pipeline the first 2 things that we need is an azure account and one resource group which we can create for free if we have a credit card else you can make use of your company’s account as well.

The architecture diagram for my sample pipeline is given below:
![Alt text](architecture.png?raw=true "Title")


The technology stack which we will be covering in our pipeline are as follows:

=>Storage account to create blob storage container to store input data

=>Azure data factory to create, schedule and monitor our pipeline

=>SQL Storage to create a SQL server database to store our input OLTP data

=>HDInsight Cluster to create data lake for OLAP.

=>Logic App to send the Notification and stats to business Users

## Creating a Storage Account:
We need to create a storage account in case we want to access the variety of file system variants provided by azure to us. So we will create the azure account with the minimum specification as we are creating for noncritical application so using locally redundant storage here and will select the tier as hot as we are going to make use of it for further application.

Once storage account is created we need to choose the kind of file system we want to make use of for now I am choosing blob storage which support all the variety of files. Here we will be creating a container for our input_use_cases.

Now is the time to load the data into our newly created file system container, here we can either directly import it through the web UI or set up using azure cli.

For the azure cli option we will install the azure cli into either power shell or Unix environment if none is available we can also allocate the azure cli in web UI as well once the installation is done in my case it’s Unix. We need to run az login –u Gunja@gmail.com –p password

Hola! we are connected to azure cli. Now to upload the data in azure cli we can run the following command
```bash
Code Snippet:

blobStorageAccount="staginm13"

blobStorageAccountKey=$(az storage account keys list -g CEP-B3-M14-2020 \

-n $blobStorageAccount --query "[0].value" --output tsv)

az storage blob upload-batch --pattern "*-02-20*.csv" --source /mnt/c/Users/Admin/ubuntu/azure/deployment/storage_database_cotainer_template/CovidCases/ --destination covidcases --account-name=$blobStorageAccount --account-key=$blobStorageAccountKey
```
On successful run of the command we can verify the batch of data has already been uploaded successfully into our blob data container.

## Creating an Azure Data Factory pipeline:
ADF is platform as service solution to publish and monitor our data pipeline and it offers a great suite of services to pick for different platforms and environment, if there are some of BI guys here you can relate it to SSIS.

Our ADF pipeline will look as following:

![Alt text](adf_pipeline_snapshot.png?raw=true "Title")

So for building this ADF pipeline we have used the following services:

COPY DATA -> MAPREDUCE -> HIVE -> WEB

Let’s start building the pipeline from scratch first we will search for azure data factory and give it a name and select will set up GitHub later option and skip through all the other options to start our service.

Once the ADF has been created we will select the author and monitor option which will bring us to a new tab to design our pipelines. In the new window we will select author option and then will search for the above 4 set of services and pull it into our screen to arrange in the above pattern.

Let’s fill in the detail in the individual tasks Now,

1. Copy Task: This task is used to copy data from one platform to another platform so we are making use of this to copy our data from blob container to SQL server database for OLTP.

->In The source we will be creating a linked service to connect to blob storage and a dataset to point to our input file csv files with first row as header option. In the source option select a wildcard file path to *.csv

->In the sync we will be creating a similar linked service connection to connect to our SQL server and a dataset to point to our destination table. And we will pass a prescript “DELETE FROM COVID_CASES”

->In the mapping we will import schema and make sure our tables columns are pointing to correct input csv file columns.

2. MapReduce Task: This task is used to run java code on our HDInsight clusters as we don’t have any way to run the sqoop jobs using azure data factory directly. So we have wrapped our sqoop jobs in java jar file and sqooping our OLTP input tables in the hive data lake using this task.

=> HDI CLUSTER linked service to connect to our hdinsight cluster.

=> In jar we will create a linked service to point to our blob storage where we will store the jar file in the class name we will pass “createajarfile” and browse the jar file path to point to actual jar file.

To create the jar wrapper for sqoop task, we need to write a java code to the same for us here is the code that I had written we can refer else we can .
```java
Code Snippet: vi CreateAJarFile.java

import java.io.*;

public class CreateAJarFile {

public static void main(String[] args) {

String content = "sqoop import --connect 'jdbc:sqlserver://serverm13.database.windows.net:1433;database=databasem13' --username batchm13 --password somepasswordD1! --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --table COVID_CASES --hcatalog-table covid_Cases --split-by rowkey -m 1";

// If the file doesn't exists, create and write to it

// If the file exists, truncate (remove all content) and write to it

try (FileWriter writer = new FileWriter("app.log");

BufferedWriter bw = new BufferedWriter(writer)) {

bw.write(content);

} catch (IOException e) {

System.err.format("IOException: %s%n", e);

}

execute("/bin/chmod u+x app.log");

execute("/bin/sh app.log");

}

private static void execute(String command) {

try {

Process p = Runtime.getRuntime().exec(command);

BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));

String line = "";

while ((line = input.readLine()) != null) {

System.out.println(line);

}

input.close();

} catch (Exception e) {

e.printStackTrace();

}

}

}
```
```bash
vi CreateAJarFile.java-> javac CreateAJarFile.java-> jar –cvf MyJarFile.jar CreateAJarFile.class

if you want to test the run, the jar file: java –cp MyJarFile.jar CreateAJarFile.
```
3. Hive Task: This task is used to run our hive query language scripts on the HDInsight cluster, and we pass on the blob location of our scripts and this task will run the passed hql on our clusters for manipulating and generating data for our OLAP tables.
```bash
Code snippet:

az storage blob upload-batch --pattern "hiveDml.hql" --source /mnt/c/Users/Admin/ubuntu/azure/deployment/final_deployment/ --destination containerm13 --account-name=$blobStorageAccount --account-key=$blobStorageAccountKey
```
hiveDml.hql
```sql
DROP TABLE COVID_OUTPUT;

INSERT OVERWRITE TABLE COVID_CASES SELECT DISTINCT * FROM COVID_CASES;

CREATE EXTERNAL TABLE COVID_OUTPUT(state varchar(50), Cured varchar(20), Deaths varchar(20), Confirmed varchar(20) ) ROW FORMAT DELIMITED

FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION 'wasbs://outputhive@staginm13.blob.core.windows.net/outputhive';

INSERT OVERWRITE TABLE COVID_OUTPUT select state,sum(cast (cured as int)),sum(cast (deaths as int)),sum(cast (confirmed as int) ) from covid_cases group by state;
```
4. Web Task: Used to hit a web URL to communicate some information from a public or private url. We will be making use of this task to send out the dynamic details of pipeline run to our azure’s logic app service which will send the mail to our end business users.

In this task we will be filling out the following option in settings,

URL: the web url which we will be generating in the logic app service.

Method: POST

Body: here we will paste the below code component in the code snippet portion.
```json
Code snippet:

{

"title": "@{pipeline().Pipeline} SUCCEEDED!",

"message": "Pipeline run finished successfully!",

"color": "Green",

"dataFactoryName": "@{pipeline().DataFactory}",

"pipelineName": "@{pipeline().Pipeline}",

"pipelineRunId": "@{pipeline().RunId}",

"time": "@{utcnow()}"

}
```
And we will create a similar web task for failure notification as well with the same steps as above.

## Creating a SQL database: 
This service is used to create a SQL server database to provide a SQL storage for OLTP operations. We will be creating a database account a new SQL server and set the admin login credentials for DB operations. Try to configure the database for minimum operations as per individual requirement.

Once the service is started while trying to logging it will ask to set up firewall, click on that link and select add client IP and click on save then retry to login you will be able to successfully login this time. Now we will open our query editor to create DB table to store our input data.
```sql
Code Snippet:

DROP TABLE IF EXISTS COVID_CASES;

CREATE TABLE COVID_CASES (Sno INT, DateCol varchar(20), TimeCol varchar(20), State varchar(50), ConfirmedIndianNational varchar(20), ConfirmedForeignNational varchar(20), Cured varchar(20), Deaths varchar(20), Confirmed varchar(20) );
```
## Creating a HDInsight Cluster: 
This service is used to Create different variants of big data clusters we have created a Hadoop big data cluster for our OLAP database using this service. We will select the minimum viable option of one worker node for our pipeline you can increase the number of nodes as per your requirement. In this service for creating a cluster We allocate a bunch of processing nodes and storage account for storing our input HDFS files if you already have created a storage account you can point to that else we can create a new data lake storage account while creating the HDInsight cluster.

Once the cluster is created we can open the ambari view and in top right we can select hiveserver2 to run query editor for hive, here we will create the table to sqoop our data from SQL server.
```sql
Code Snippet:

DROP TABLE IF EXISTS COVID_CASES;

CREATE TABLE COVID_CASES (Sno INT, DateCol varchar(20), TimeCol varchar(20), State varchar(50), ConfirmedIndianNational varchar(20), ConfirmedForeignNational varchar(20), Cured varchar(20), Deaths varchar(20), Confirmed varchar(20) );
```
we can also connect to hive using azure cli using the following commands.
```bash
ssh sshuser@clusterm13gunish-ssh.azurehdinsight.net

/usr/bin/hive
```
And then run the create table statements to create required DDL structure for our sqooping tables.

As an alternative for creating the service through web ui we can also use the Automation code to create a cluster service directly using CLI
```bash
Code Snippet:

az hdinsight create \

--name clusterm13gunish \

--resource-group CEP-B3-M13-2020 \

--type hadoop \

--component-version Hadoop=2.7 \

--http-password 12345@ABC \

--http-user admin \

--location EASTUS \

--workernode-count 1 \

--ssh-password somepasswordD1! \

--ssh-user sshuser \

--storage-account staginm13 \

--storage-account-key $(az storage account keys list -g CEP-B3-M14-2020 -n staginm13 --query "[0].value" --output tsv) \

--storage-container containerm13
```
## Creating a Logic app service:
This service is used to send the custom notification to our end users and we will be creating this service to send our email notifications to business users of pipeline completion status.

In this service we will be creating 3 components when an HTTP request is received, initialize variable task and send an email task.

We will be making use of URL generated in the HTTP request in the azure data factory->web->url option. In this task once we save the web url is generated and we will pass the body with the option use sample payload to generate schema.
```json
Code snippet:

{

"title": "",

"message": "",

"color": "",

"dataFactoryName": "",

"pipelineName": "",

"pipelineRunId": "",

"time": ""

}
```
We need to add the variable action and then choose the initialize variable. In the name we will pass it as email body and in the type as string in the value we will paste the below shared snippet and replace the blank spaces with the dynamic content available corresponding to each blank values.
```html
Code snippet:
l
<hr/>

<h2 style='color:____color_here____'>____title_here____</h2>

<hr/>

Data Factory Name: <b>____name_here____</b><br/>

Pipeline Name: <b>____name_here____</b><br/>

Pipeline Run Id: <b>____id_here____</b><br/>

Time: <b>____time_here____</b><br/>

<hr/>

Information<br/>

<p style='color:____color_here____'>____message_here____</p>

<hr/>

<p style='color:gray;'>This email was generated automatically. Please do not respond to it. Contact team at: contact@gunish.com </p>
```
In the send mail task, we will set the mail recipients and configure the sender’s mail id and in the body we will pass the content of expression task we had created the step above. And in subject we will select the title from right dynamic content options available.

## Automating the deployment pipeline:
we can download the ARM template from azure for all the resources combined by exporting the ARM template of the resource group if we have used the same resource group to create all the components.
```bash
Code Snippet

az deployment group create \

--name ExampleDeployment \

--resource-group CEP-B3-M13-2020 \

--template-file template.json \

--parameters @parameter.json
```
I have covered all the components in summarized manner so that they can help you to quickly get started over the Azure cloud components. Will be coming back with similar condensed snippets in future up till then good bye.

<!-- Actual text -->

You can find me on [![Twitter][1.2]][1].

<!-- Icons -->

[1.2]: http://i.imgur.com/wWzX9uB.png (twitter icon without padding)


<!-- Links to your social media accounts -->

[1]: https://twitter.com/gunishjha

