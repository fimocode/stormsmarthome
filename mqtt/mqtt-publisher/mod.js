var file_path = "../data_file/house-1.csv";
var file_offset = 0;
var line_offset = 0;
var start_publish = false;
var unlimit = false;
var hwm = 200;
var total = 0;
var broker_url = "mqtt-broker"
var topic = "iot-data"
var lineReader;
var file_stream; 
var lrPos;
var client;
const interval = 1000;

require('events').EventEmitter.defaultMaxListeners = 0;

const options = process.argv.slice(2);

const file_path_toogle = options.indexOf('-f')
const hwm_toogle = options.indexOf('-h');
const broker_url_toogle = options.indexOf('-b');
const topic_toogle = options.indexOf('-t');
const original_data_toogle = options.indexOf('-o');
const line_offset_toogle = options.indexOf('-r');
const date_change_toogle = options.indexOf('-d');
const qos_change_toogle = options.indexOf('-q');
var gap = 0;  //sec
var mqtt_options = {
    qos: 0,
    retain: false
}
var previous_timestamp = null;  // To track the previous timestamp for delay

async function main(){
    if(file_path_toogle!=-1){
        file_path = options[file_path_toogle+1];
    }
    if(!require('fs').existsSync(file_path)){
        console.error(`File ${file_path} not found`);
        process.exit(1);
    }

    if(hwm_toogle!=-1){
        if(!isNaN(options[hwm_toogle+1]) && options[hwm_toogle+1]>0){
            hwm = parseInt(options[hwm_toogle+1]);
            console.log("HWM set: " + hwm);
        }
        else{
            console.error("Wrong hwm value");
            process.exit(1);
        }
    }

    if(qos_change_toogle!=-1 && options[qos_change_toogle+1]>=0 && options[qos_change_toogle+1]<=2){
        mqtt_options.qos = parseInt(options[qos_change_toogle+1]);
        console.log("QoS set to " + mqtt_options.qos);
    }

    if(original_data_toogle!=-1){
        console.log("Using original data");
    }
    else if(date_change_toogle!=-1){
        console.log("Using change date only");
    }
    else {
        console.log("Using realtime timestamp");
    }

    if(broker_url_toogle!=-1){
        broker_url = options[broker_url_toogle+1];
        console.log("Broker set : " + broker_url);
    }
    
    if(topic_toogle!=-1){
        topic = options[topic_toogle+1];
        console.log("Topic set: " + topic);
    }

    if(line_offset_toogle!=-1){
        if(!isNaN(options[line_offset_toogle+1])){
            line_offset=parseInt(options[line_offset_toogle+1]);
            console.log(`Start from line offset: ${options[line_offset_toogle+1]}`);
        }
        else{
            console.log("Offset file wrong value. Offset value will be 0 by default");
        }
    }
    
    client = require('mqtt').connect(`mqtt://${broker_url}`);

    var break_line = Array(process.stdout.columns).join('*');

    file_offset = await checkLastRun();

    lrPos+=file_offset;

    console.log(`File: ${file_path} | HWM: ${hwm}\nBroker URL: ${broker_url}\n${break_line}`);

    client.on('connect', function () {
        console.log("Connected to broker");
        file_stream = require('fs').createReadStream(file_path, {
            start: file_offset,
            highWaterMark: hwm
        });

        lineReader = require('readline').createInterface({
            input: file_stream
        });

        setInterval(()=>{
            if(lineReader){
                lineReader.resume();
            }
            let readline = require('readline');
            readline.clearLine(process.stdout, 0);
            readline.cursorTo(process.stdout, 0);
            process.stdout.write(`Total sent: ${total}\r`);
            if(start_publish){
                require('fs').writeFileSync('.publish.stat', `${process.argv}|${file_offset+lrPos}|${total}|0|`);
            }
            speed=0;
        },interval);
        
        lineReader.on('line', line => {
            if(total>=line_offset){
                lineReader.pause();
                start_publish = true;

                let values = line.split(',');
                let current_timestamp = parseInt(values[1]);  // Extract the timestamp

                if (previous_timestamp !== null) {
                    // Calculate the delay based on the timestamp difference
                    let delay = (current_timestamp - previous_timestamp) * 1000;
                    if (delay > 2147483647) {
                        console.log(delay)
                    }
                    setTimeout(() => {
                        client.publish(topic, line, mqtt_options, (err,pkt)=>{
                            total++;
                            previous_timestamp = current_timestamp;  // Update previous timestamp
                            lineReader.resume();
                        });
                    }, delay);
                } else {
                    // First line, no delay
                    client.publish(topic, line, mqtt_options, (err,pkt)=>{
                        total++;
                        previous_timestamp = current_timestamp;  // Initialize the previous timestamp
                        lineReader.resume();
                    });
                }
                lrPos = file_stream.bytesRead
            }
            total++;
        });
    })
}

async function checkLastRun(){
    return new Promise(resolve=>{
        let exist = require('fs').existsSync('.publish.stat');
        if(exist){
            let data = require('fs').readFileSync('.publish.stat').toString();
            let offset = isNaN(data.split('|')[1])?0:parseInt(data.split('|')[1]);
            console.log(`Last run was end at ${offset}`)
            resolve(offset);
        }
        else{
            resolve(0);
        }
    })
}

setTimeout(main,1000);
