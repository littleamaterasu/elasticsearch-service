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
    switch (message.topic) {
        case inTopics[0]:
            result = await getPersonalData(message.value);  // Gọi hàm xử lý A
            break;
        case inTopics[1]:
            result = await search(message.value);  // Gọi hàm xử lý B
            break;
        default:
            console.log('Không có hành động cho topic này:', message.topic);
    }

    if (result) {
        const outputTopic = outTopics[inTopics.indexOf(message.topic)];
        producer.send([{ topic: outputTopic, messages: result }], (err, res) => {
            if (err) {
                console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outputTopic}:`, err);
            } else {
                console.log(`Dữ liệu đã được gửi thành công tới ${outputTopic}:`, res);
            }
        });
    }
})

// Lắng nghe lỗi
consumer.on('error', function (err) {
    console.error('Lỗi Kafka Consumer:', err);
});

// Lắng nghe khi consumer kết nối thành công
consumer.on('connect', function () {
    console.log('Consumer đã kết nối với Kafka');
});