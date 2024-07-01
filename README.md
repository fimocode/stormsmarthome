# stormsmarthome
Storm topology to subscribt MQTT broker and analyze smarthome data


## How to create storm topology jar file for development/test or del

**Create enviroment variable for topo**

```cmd
cp sample_env.yaml src/main/resources/config/cred.yaml
```

<b>Build with maven</b>
```cmd
mvn install
```
<b>Run and test (Run with windows size 5,10,15,20,30,60,120 mins)</b>

```cmd
java -jar target/Storm-IOTdata-1.0-SNAPSHOT-jar-with-dependencies.jar
```
  
Then choose data file.

Note: Output files will be at the same folder with input file.


## How to create storm cluster by docker compose

**Run all storm cluster**

```cmd
docker compose up -d
```

**Then apply iot-data-smarthome topo**

```cmd
docker exec -it nimbus bash

storm jar /target/Storm-IOTdata-1.0-SNAPSHOT-jar-with-dependencies.jar com.storm.iotdata.MainTopo
```

