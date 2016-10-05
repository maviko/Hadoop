import com.jayway.jsonpath.JsonPath
import groovy.json.JsonSlurper
import org.apache.hadoop.gateway.shell.Hadoop
import org.apache.hadoop.gateway.shell.hdfs.Hdfs
import org.apache.hadoop.gateway.shell.workflow.Workflow

import static java.util.concurrent.TimeUnit.SECONDS



// Host, userid & password taken from VCAP_Services
gateway = "*********"
username = "*********"
password = "*********"

jobDir = "/user/" + username + "/folder"
inputFile = "*********"
jarFile = "********"


// i) Identify and define the core parameters for workflow
definition = """\
<workflow-app xmlns="uri:oozie:workflow:0.2" name="wordcount-workflow">
    <start to="root-node"/>
    <action name="root-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <main-class>org.apache.hadoop.examples.WordCount</main-class>
            <arg>\${inputDir}</arg>
            <arg>\${outputDir}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
"""

// ii) Provide suitable values to these parameters
configuration = """\
<configuration>
    <property>
        <name>user.name</name>
        <value>default</value>
    </property>
    <property>
        <name>nameNode</name>
        <value>default</value>
    </property>
    <property>
        <name>jobTracker</name>
        <value>default</value>
    </property>
    <property>
        <name>inputDir</name>
        <value>$jobDir/input</value>
    </property>
    <property>
        <name>outputDir</name>
        <value>$jobDir/output</value>
    </property>
    <property>
        <name>oozie.wf.application.path</name>
        <value>$jobDir</value>
    </property>
</configuration>
"""

// create session
session = Hadoop.login( gateway, username, password )
println "\nSession opened. "

// iii) Create folders and upload necessary files in HDFS thru WebHDFS API
// Cleanup and recreate /user/biblumix/testdir folder

println "\nDrop folder - " + jobDir + ": " + Hdfs.rm( session ).file( jobDir ).recursive().now().statusCode
println "\nCreate folder - " + jobDir + ": " + Hdfs.mkdir( session ).dir( jobDir ).now().statusCode

// Uploading TweetText.txt with different name - SampleFile.txt
putData = Hdfs.put(session).file( inputFile ).to( jobDir + "/input/SampleFile.txt" ).later() {
    println "\nUpload TweetText.txt - " + jobDir + "/input/SampleFile.txt: " + it.statusCode
}

// Uploading jar file
putJar = Hdfs.put(session).file( jarFile ).to( jobDir + "/lib/hadoop-examples.jar" ).later() {
    println "\nUpload hadoop-examples.jar - " + jobDir + "/lib/hadoop-examples.jar: " + it.statusCode
}

// Uploading workflow.xml file
putWorkflow = Hdfs.put(session).text( definition ).to( jobDir + "/workflow.xml" ).later() {
    println "\nUpload workflow.xml -  " + jobDir + "/workflow.xml: " + it.statusCode
}

session.waitFor( putWorkflow, putData, putJar )

// iv) Run Oozie workflow
jobId = Workflow.submit(session).text( configuration ).now().jobId
println "\nOozie job Submitted - : " + jobId
println "\n******* TweetText.txt File content - \n" + Hdfs.get( session ).from( jobDir + "/input/SampleFile.txt" ).now().string

println "\n****** Polling up to 60s for job completion..."
status = "RUNNING";
count = 0;
while( status == "RUNNING" && count++ < 600 ) {
    sleep( 1000 )
    json = Workflow.status(session).jobId( jobId ).now().string
    status = JsonPath.read( json, "\$.status" )
    print "."; System.out.flush();
}

println ""
println "\nJob status: " + status

if( status == "SUCCEEDED" ) {
    text = Hdfs.ls( session ).dir( jobDir + "/output" ).now().string
    json = (new JsonSlurper()).parseText( text )
    println json.FileStatuses.FileStatus.pathSuffix
    println "\n***** Resultant File content : \n" + Hdfs.get( session ).from( jobDir + "/output/part-r-00000" ).now().string
}

// Close session
println "\nSession closed. " + session.shutdown( 10, SECONDS )