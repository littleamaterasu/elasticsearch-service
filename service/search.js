const { Client } = require('@elastic/elasticsearch');
require('dotenv').config();

// Cấu hình Elasticsearch
const elasticsearchClient = new Client({ node: `http://${process.env.ES_URL}` });
const elasticsearchIndexName = 'crawled-stock-data';

// Hàm truy vấn Elasticsearch
const search = async (parsedValue) => {
    const keywords = parsedValue.keywords;
    const from = parsedValue.from;
    const to = parsedValue.to;
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
        const hits = result.hits.hits.map(hit => hit._source);

        parsedValue.result = hits;
        return parsedValue; // Trả về kết quả cho người gọi
    } catch (error) {
        console.error('Elasticsearch Query Error:', error);
        throw error;
    }
};

module.exports = { search };
