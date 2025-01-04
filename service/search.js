const { Client } = require('@elastic/elasticsearch');

// Cấu hình Elasticsearch
const elasticsearchClient = new Client({ node: elasticsearchUrl });

// Hàm truy vấn Elasticsearch
const search = async (keywords, from, to) => {
    try {
        // Truy vấn Elasticsearch
        const result = await elasticsearchClient.search({
            index: elasticsearchIndexName,
            body: {
                query: keywords === '*'
                    ? { match_all: {} }  // Nếu từ khóa là '*', trả về tất cả tài liệu
                    : {
                        match: {
                            keywords: keywords // Tìm theo từ khóa
                        }
                    },
                sort: [
                    {
                        _score: {  // Sắp xếp theo độ tương đồng (score)
                            order: 'desc'
                        }
                    }
                ],
                from: from,   // Điểm bắt đầu cho phân trang
                size: to - from,  // Số lượng kết quả muốn nhận
            }
        });

        // Trích xuất dữ liệu từ kết quả trả về
        const hits = result.body.hits.hits.map(hit => hit._source);

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

        return hits; // Trả về kết quả cho người gọi
    } catch (error) {
        console.error('Elasticsearch Query Error:', error);
        throw error;
    }
};

module.exports = { search };
