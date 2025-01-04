const { KafkaClient, Consumer, Producer } = require('kafka-node');
const { getPersonalData } = require('./service/personalData');
const { search } = require('./service/search');

const kafkaClient = new KafkaClient({ kafkaHost: 'localhost:9092' });
const producer = new Producer(kafkaClient);

const inTopics = ['personal-data', 'search'];
const outTopics = ['personal-data-res', 'search-res'];

const consumer = new Consumer(kafkaClient, inTopics.map(topic => ({ topic, partition: 0 })), { autoCommit: true });

consumer.on('message', async (message) => {
    console.log(`Nhận message từ topic ${message.topic}:`, message.value);

    let result;
    try {
        let parsedValue = JSON.parse(message.value); // Parse the message value if it's a JSON string

        switch (message.topic) {
            case inTopics[0]:
                if (parsedValue.uid && parsedValue.jobId)
                    result = await getPersonalData(parsedValue);  // Gọi hàm xử lý A
                break;
            case inTopics[1]:
                if (parsedValue.keywords && parsedValue.from && parsedValue.to && parsedValue.jobId)
                    result = await search(parsedValue);  // Gọi hàm xử lý B
                break;
            default:
                console.log('Không có hành động cho topic này:', message.topic);
        }

        if (result) {
            const outputTopic = outTopics[inTopics.indexOf(message.topic)];
            producer.send([{ topic: outputTopic, messages: JSON.stringify(result) }], (err, res) => {
                if (err) {
                    console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outputTopic}:`, err);
                } else {
                    console.log(`Dữ liệu đã được gửi thành công tới ${outputTopic}:`, res);
                }
            });
        }
    } catch (error) {
        console.error(`Lỗi xử lý message từ topic ${message.topic}:`, error);
    }
});

// Lắng nghe lỗi
consumer.on('error', function (err) {
    console.error('Lỗi Kafka Consumer:', err);
});

// Lắng nghe khi consumer kết nối thành công
consumer.on('connect', function () {
    console.log('Consumer đã kết nối với Kafka');
});
