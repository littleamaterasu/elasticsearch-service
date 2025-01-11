const { KafkaClient, Consumer, Producer } = require('kafka-node');
const { getPersonalData } = require('./service/personalData');
const { search } = require('./service/search');
const { saveToES, tokenize } = require('./service/elasticsearchClient');

const kafkaClient = new KafkaClient({ kafkaHost: process.env.KAFKA_HOST });
const producer = new Producer(kafkaClient);

const inTopics = ['personal-data', 'search', 'crawled-data', 'classification'];
const outTopics = ['api-res', 'query', 'training-data'];

const consumer = new Consumer(kafkaClient, inTopics.map(topic => ({ topic, partition: 0 })), { autoCommit: true });

consumer.on('message', async (message) => {
    console.log(`Nhận message từ topic ${message.topic}`);
    try {
        let parsedValue = JSON.parse(message.value); // Parse the message value if it's a JSON string

        switch (message.topic) {
            case inTopics[0]:
                if (parsedValue.uid && parsedValue.jobId) {
                    const result = await getPersonalData(parsedValue);  // Gọi hàm xử lý A
                    if (result)
                        producer.send([{ topic: outTopics[0], messages: JSON.stringify(result) }], (err, res) => {
                            if (err) {
                                console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outputTopic}:`, err);
                            } else {
                                console.log(`Dữ liệu đã được gửi thành công tới ${outputTopic}:`, res);
                            }
                        });
                }
                break;

            case inTopics[1]:
                if (parsedValue.keywords && parsedValue.from && parsedValue.to && parsedValue.jobId) {

                    // ---Kết quả tìm kiếm---
                    const result = await search(parsedValue);  // Gọi hàm xử lý B
                    if (result)
                        producer.send([{ topic: outTopics[0], messages: JSON.stringify(result) }], (err, res) => {
                            if (err) {
                                console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outputTopic}:`, err);
                            } else {
                                console.log(`Dữ liệu đã được gửi thành công tới ${outputTopic}:`, res);
                            }
                        });

                    // ---Logs user---
                    // Thêm timestamp
                    parsedValue.timeStamp = Date.now();

                    // Lưu parsedValue vào Elasticsearch
                    await saveToES([
                        {
                            keywords: parsedValue.keywords,
                            uid: parsedValue.uid,
                            timeStamp: parsedValue.timeStamp
                        }
                    ], 'stock-logs-index');

                    // Tokenize keywords
                    const tokens = await tokenize(parsedValue.keywords);

                    // Chuẩn bị kết quả để gửi tới Kafka
                    const logs = {
                        uid: parsedValue.userId,
                        tokens: tokens
                    };

                    // Gửi dữ liệu tới Kafka
                    producer.send([{ topic: outTopics[2], messages: JSON.stringify(logs) }], (err, res) => {
                        if (err) {
                            console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outTopics[2]}:`, err);
                        } else {
                            console.log(`Dữ liệu đã được gửi thành công tới ${outTopics[2]}:`, res);
                        }
                    });
                }
                break;

            case inTopics[2]:
                if (parsedValue) {
                    await saveToES([parsedValue], 'crawled-stock-index');
                    const tokens = tokenize(parsedValue.title + ', ' + parsedValue.keywords + ', ' + parsedValue.description + ', ' + parsedValue.content);
                    const keywords = parsedValue.keywords.split(', ');
                    const result = {
                        tokens: tokens,
                        keywords: keywords
                    }
                    producer.send([{ topic: outTopics[2], messages: JSON.stringify(result) }], (err, res) => {
                        if (err) {
                            console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outputTopic}:`, err);
                        } else {
                            console.log(`Dữ liệu đã được gửi thành công tới ${outputTopic}:`, res);
                        }
                    });
                }
                break;

            case inTopics[3]:
                if (parsedValue) {
                    await saveToES(parsedValue, 'prefrence-index');
                }
                break;

            default:
                console.log('Không có hành động cho topic này:', message.topic);
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
