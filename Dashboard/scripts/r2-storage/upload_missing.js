const fs = require('fs');
const path = require('path');
const https = require('https');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
require('dotenv').config();

const missingFilesList = require('../missing_files.json'); // Array chứa các file path
const bucketName = process.env.R2_BUCKET_NAME; 
const sourceDataDir = path.join(__dirname, '../data');

async function getServerTime() {
    return new Promise((resolve, reject) => {
        https.get('https://cloudflare.com', (res) => {
            const dateStr = res.headers.date;
            resolve(dateStr ? new Date(dateStr) : new Date());
        }).on('error', (e) => reject(e));
    });
}

function createClientWithSystemClockOffset(serverTime) {
    const offset = serverTime.getTime() - Date.now();
    return new S3Client({
        region: 'auto',
        endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: process.env.R2_ACCESS_KEY_ID,
            secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
        },
        systemClockOffset: offset,
        maxAttempts: 3
    });
}

async function uploadFile(s3Client, fileRelativePath) {
    const filePath = path.join(sourceDataDir, fileRelativePath);
    const fileBuffer = fs.readFileSync(filePath);
    const r2Key = fileRelativePath; // Vì JSON in ra sẵn dùng '/', chuẩn key R2 luôn

    const uploadParams = {
        Bucket: bucketName,
        Key: r2Key,
        Body: fileBuffer,
    };

    try {
        await s3Client.send(new PutObjectCommand(uploadParams));
        console.log(`✅ File đã được cứu: ${r2Key}`);
        return true;
    } catch (err) {
        console.error(`❌ Khóc, vẫn rớt file ${r2Key}:`, err.message);
        return false;
    }
}

async function retryMissing() {
    if (missingFilesList.length === 0) {
        console.log("Không có file nào bị miss!");
        return;
    }

    console.log("Đang lấy giờ Cloudflare chuẩn...");
    const serverTime = await getServerTime();
    const s3Client = createClientWithSystemClockOffset(serverTime);

    console.log(`\n⏳ Đang upload bù lại ${missingFilesList.length} files...`);
    
    let successCount = 0;
    for (const r2Key of missingFilesList) {
        const success = await uploadFile(s3Client, r2Key);
        if (success) successCount++;
        
        await new Promise(resolve => setTimeout(resolve, 800)); // Delay lâu một chút cho chắc
    }

    console.log(`\n🎉 Xong nhiệm vụ! Đã fix lỗi ${successCount}/${missingFilesList.length} files.`);
}

retryMissing().catch(console.error);
