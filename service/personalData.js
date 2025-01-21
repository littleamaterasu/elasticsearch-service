const { Client } = require('@elastic/elasticsearch');
require('dotenv').config();

// Cấu hình Elasticsearch
const elasticsearchClient = new Client({ node: `http://${process.env.ES_URL}` });
const personalDataIndex = 'personal-data';
const stockDataIndex = 'crawled-stock-data';

// Hàm truy vấn Elasticsearch
const getPersonalData = async (parsedValue) => {
    const uid = parsedValue.uid;
    try {
        // Truy vấn dữ liệu từ chỉ mục personal-data
        const result = await elasticsearchClient.search({
            index: personalDataIndex,
            body: {
                query: {
                    match: {
                        uid: uid // Tìm theo uid
                    }
                },
                sort: [
                    { timeStamp: { order: 'desc' } } // Sắp xếp timestamp giảm dần
                ],
                size: 50 // Giới hạn kết quả trả về là 50
            }
        });

        // Trích xuất dữ liệu từ kết quả trả về
        console.log('hits', result.hits.hits)
        if (result.hits.hits.length === 0) {
            parsedValue.result = [];
            return parsedValue;
        }
        const hits = result.hits.hits.map(hit => hit._source);
        // Đếm số lần xuất hiện của mỗi keyword
        const keywordCount = hits.reduce((acc, item) => {
            const keyword = item.keywords;
            if (keyword) {
                acc[keyword] = (acc[keyword] || 0) + 1;
            }
            return acc;
        }, {});

        // Lấy keyword phổ biến nhất (tần suất cao nhất)
        const topKeyword = Object.keys(keywordCount)
            .sort((a, b) => keywordCount[b] - keywordCount[a])[0]; // Lấy keyword đứng đầu

        // Nếu không có keyword, trả về kết quả rỗng
        if (!topKeyword) {
            parsedValue.result = [];
            return parsedValue;
        }

        // Truy vấn dữ liệu từ chỉ mục crawled-stock-data với keyword phổ biến nhất
        const stockDataResult = await elasticsearchClient.search({
            index: stockDataIndex,
            body: {
                query: {
                    match: {
                        keywords: topKeyword // Tìm kiếm theo từ khóa phổ biến nhất
                    }
                },
                size: 50 // Giới hạn kết quả trả về là 50
            }
        });

        // Trích xuất dữ liệu từ kết quả trả về
        const stockDataHits = stockDataResult.hits.hits.map(hit => hit._source);

        // Gắn kết quả vào parsedValue
        parsedValue.result = stockDataHits;

        return parsedValue; // Trả về kết quả cho người gọi
    } catch (error) {
        console.error('Elasticsearch Query Error:', error);
        throw error;
    }
};

module.exports = { getPersonalData };
