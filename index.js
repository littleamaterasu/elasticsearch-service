const { KafkaClient, Consumer, Producer } = require('kafka-node');
const { getPersonalData } = require('./service/personalData');
const { search } = require('./service/search');
const { saveToES, tokenize } = require('./service/elasticsearchClient');

const kafkaClient = new KafkaClient({ kafkaHost: process.env.KAFKA_HOST });
const producer = new Producer(kafkaClient);

const inTopics = ['preference', 'search', 'crawled-data', 'classification'];
const outTopics = ['api-res', 'query', 'training-data'];

const consumer = new Consumer(
    kafkaClient,
    inTopics.map(topic => ({ topic, partition: 0 })),
    {
        autoCommit: true, // Vô hiệu hóa auto commit offset
    }
);

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
                                console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outTopics[0]}:`, err);
                            } else {
                                console.log(`Dữ liệu đã được gửi thành công tới ${outTopics[0]}:`, res);
                            }
                        });
                }
                break;

            case inTopics[1]:
                if (parsedValue.keywords && parsedValue.from && parsedValue.to && parsedValue.jobId && parsedValue.uid) {

                    // ---Kết quả tìm kiếm---
                    const result = await search(parsedValue);  // Gọi hàm xử lý B
                    if (result)
                        producer.send([{ topic: outTopics[0], messages: JSON.stringify(result) }], (err, res) => {
                            if (err) {
                                console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outTopics[0]}:`, err);
                            } else {
                                console.log(`Dữ liệu đã được gửi thành công tới ${outTopics[0]}:`, res);
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
                    const tokens = await tokenize(parsedValue.keywords, 'prj3', 'my_vi_analyzer');

                    // Chuẩn bị kết quả để gửi tới Kafka
                    const logs = {
                        uid: parsedValue.uid,
                        tokens: tokens
                    };

                    console.log(logs);

                    // Gửi dữ liệu tới Kafka
                    producer.send([{ topic: outTopics[1], messages: JSON.stringify(logs) }], (err, res) => {
                        if (err) {
                            console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outTopics[1]}:`, err);
                        } else {
                            console.log(`Dữ liệu đã được gửi thành công tới ${outTopics[1]}:`, res);
                        }
                    });
                }
                break;

            case inTopics[2]:
                if (parsedValue) {
                    console.log('keywords', parsedValue.keywords)
                    await saveToES([parsedValue], 'prj3');
                    const tokens = await tokenize(parsedValue.title + ', ' + parsedValue.keywords.join(', ') + ', ' + parsedValue.description + ', ' + parsedValue.content, 'prj3', 'my_vi_analyzer');
                    const result = {
                        tokens: tokens,
                        keywords: parsedValue.keywords
                    }
                    producer.send([{ topic: outTopics[2], messages: JSON.stringify(result) }], (err, res) => {
                        if (err) {
                            console.error(`Lỗi gửi dữ liệu tới Kafka topic ${outTopics[2]}:`, err);
                        } else {
                            console.log(`Dữ liệu đã được gửi thành công tới ${outTopics[2]}:`, res);
                        }
                    });
                }
                break;

            case inTopics[3]:
                if (parsedValue) {
                    const classes = parsedValue.topIndex.map(classItem => ({
                        uid: parsedValue.uid,
                        keywords: classItem,
                        timeStamp: parsedValue.timestamp
                    }));
                    console.log('classification logs', classes)
                    await saveToES(classes, 'personal-data');
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
