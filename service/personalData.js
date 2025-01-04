const { Client } = require('@elastic/elasticsearch');

// Cấu hình Elasticsearch
const elasticsearchClient = new Client({ node: elasticsearchUrl });
const elasticsearchIndexName = 'personal-data';
// Hàm truy vấn Elasticsearch
const getPersonalData = async (parsedValue) => {
    const uid = parsedValue.uid;
    try {
        // Truy vấn Elasticsearch
        const result = await elasticsearchClient.search({
            index: elasticsearchIndexName,
            body: {
                query: {
                    match: {
                        uid: uid // Tìm theo uid
                    }
                },
                sort: [
                    { timestamp: { order: 'desc' } } // Sắp xếp timestamp giảm dần
                ],
                size: 50 // Giới hạn kết quả trả về là 50
            }
        });

        // Trích xuất dữ liệu từ kết quả trả về
        const hits = result.hits.hits.map(hit => hit._source);

        // Gửi dữ liệu đến Kafka
        for (const data of hits) {
            producer.send(
                [{ topic: kafkaTopic, messages: JSON.stringify(data) }],
                (err, data) => {
                    if (err) console.error('Kafka Error:', err);
                    else console.log('Data sent to Kafka:', data);
                }
            );
        }

        parsedValue.result = hits;
        return parsedValue; // Trả về kết quả cho người gọi
    } catch (error) {
        console.error('Elasticsearch Query Error:', error);
        throw error;
    }
};

module.exports = { getPersonalData };
