const { Client } = require('@elastic/elasticsearch');
require('dotenv').config();

// Cấu hình Elasticsearch
const elasticsearchClient = new Client({ node: `http://${process.env.ES_URL}` });

/**
 * Lưu dữ liệu vào Elasticsearch sử dụng helpers.bulk.
 * @param {Array} data - Mảng chứa các tài liệu cần lưu.
 * @param {string} indexName - Tên của index.
 */

const saveToES = async (data, indexName) => {
    if (!Array.isArray(data) || data.length === 0) {
        console.error('Dữ liệu phải là một mảng không rỗng!');
        return;
    }

    try {
        const bulkPayload = [];

        data.forEach((doc) => {

            bulkPayload.push({ index: { _index: indexName } });
            bulkPayload.push(doc);
        });

        await elasticsearchClient.bulk({ body: bulkPayload });

    } catch (error) {
        console.error('Error saving to Elasticsearch:', error);
    }
};

/**
 * Tokenize một chuỗi sử dụng API analyze của Elasticsearch.
 * @param {string} text - Chuỗi cần tokenize.
 * @param {string} analyzer - Tên analyzer của Elasticsearch (vd: "standard", "whitespace").
 */
const tokenize = async (text, index, analyzer) => {
    if (typeof text !== 'string' || text.trim() === '') {
        console.error('Chuỗi truyền vào phải là một string không rỗng!');
        return;
    }

    if (typeof index !== 'string' || index.trim() === '') {
        console.error('Index phải là một string không rỗng!');
        return;
    }

    try {
        const analyzeParams = {
            index, // Tên index bạn muốn sử dụng
            text, // Chuỗi cần tokenize
        };

        // Thêm analyzer nếu được chỉ định
        if (analyzer) {
            analyzeParams.analyzer = analyzer;
        }

        const res = await elasticsearchClient.indices.analyze(analyzeParams);
        const tokens = res.tokens.map(token => token.token);

        console.log(tokens.slice(0, 2));
        return tokens;
    } catch (error) {
        console.error('Error analyzing text:', error);
    }
};

module.exports = {
    saveToES,
    tokenize
}