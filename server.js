const kafka = require('kafka-node');
let kafkaHost = 'localhost:9092';
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({ kafkaHost: kafkaHost });
const http = require("http");

http
  .createServer((request, response) => {
    //sending an event every 5 seconds
    if (request.url.toLowerCase() === "/interval_sse") {
      response.writeHead(200, {
        Connection: "keep-alive",
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        'Access-Control-Allow-Origin': '*'
      });
      let iInterval = 0;
      setInterval(() => {
        response.write("event: sendInterval\n");
        response.write('id: ' + iInterval++ + '\n');
        response.write('data: ' + (new Date()).toISOString().substring(17, 23));
        response.write("\n\n");
      }, 5000)
    }
    //sending an event for each message read from Kafka topic
    else if (request.url.toLowerCase() === "/kafka_sse") {
      response.writeHead(200, {
        Connection: "keep-alive",
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        'Access-Control-Allow-Origin': '*'
      });
      let i = 0;
      const consumer = new Consumer(
        client,
        [
          { topic: 'test' },
        ],
        {
          autoCommit: false,
          fromOffset: 'latest'
        }
      );

      consumer.on('message', function (message) {
        response.write("event: sendMsgFromKafka\n");
        response.write('id: ' + i++ + '\n');
        response.write('data: ' + message.value);
        response.write("\n\n");
      });
      consumer.on('error', function (err) {
        console.log('Error:', err);
      });

      consumer.on('offsetOutOfRange', function (err) {
        console.log('offsetOutOfRange:', err);
      });

    }
    else if (['/interval_sse', '/kafka_sse'].indexOf(request.url.toLowerCase()) < 0) {
      response.writeHead(404);
      response.end();
    }
  })
  .listen(5000, () => {
    console.log("Simplest Kafka Consumer and SSE Server running at http://127.0.0.1:5000/");
  });
