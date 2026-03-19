const fs = require('fs');
const path = require('path');
const https = require('https');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
require('dotenv').config();

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
    // Để có được Request Time hợp lệ, Clock của S3Client phải bằng với giờ server.
    // AWS SDK cộng systemClockOffset vào Date.now(): `Date.now() + offset = serverTime`
    // Tức là: offset = serverTime - Date.now()
    const offset = serverTime.getTime() - Date.now();
    console.log(`Độ lệch thời gian áp dụng cho SDK AWS: ${offset}ms`);

    const client = new S3Client({
        region: 'auto',
        endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
        credentials: {
            accessKeyId: process.env.R2_ACCESS_KEY_ID,
            secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
        },
        systemClockOffset: offset,
        maxAttempts: 2 
    });

    return client;
}

async function getFiles(dir) {
    let files = [];
    const items = await fs.promises.readdir(dir, { withFileTypes: true });

    for (const item of items) {
        if (item.name === '.DS_Store') continue;

        const fullPath = path.join(dir, item.name);
        if (item.isDirectory()) {
            files = files.concat(await getFiles(fullPath));
        } else {
            files.push(fullPath);
        }
    }
    return files;
}

async function uploadFile(s3Client, filePath) {
    const fileBuffer = fs.readFileSync(filePath);
    
    const relativePath = path.relative(sourceDataDir, filePath);
    const r2Key = relativePath.split(path.sep).join('/');

    const uploadParams = {
        Bucket: bucketName,
        Key: r2Key,
        Body: fileBuffer,
    };

    try {
        await s3Client.send(new PutObjectCommand(uploadParams));
        return { success: true, r2Key };
    } catch (err) {
        console.error(`❌ Lỗi upload ${r2Key}:`, err.message);
        return { success: false, r2Key };
    }
}

async function startUpload() {
    console.log(`Quét thư mục: ${sourceDataDir}`);
    if (!fs.existsSync(sourceDataDir)) {
         console.error(`Thư mục ${sourceDataDir} không tồn tại!`);
         return;
    }

    console.log("Đang lấy giờ chuẩn hệ thống Cloudflare...");
    const serverTime = await getServerTime();
    console.log(`Giờ server chuẩn: ${serverTime.toISOString()}`);

    const s3Client = createClientWithSystemClockOffset(serverTime);

    const allFiles = await getFiles(sourceDataDir);
    console.log(`Tìm thấy ${allFiles.length} file cần upload lên bucket: ${bucketName}.`);

    let successCount = 0;
    const BATCH_SIZE = 5; // Upload đồng thời 5 file để tránh Rate Limit

    for (let i = 0; i < allFiles.length; i += BATCH_SIZE) {
        const batch = allFiles.slice(i, i + BATCH_SIZE);
        console.log(`\n>[Tiến độ: ${i}/${allFiles.length}] Đang xử lý batch ${batch.length} files...`);
        
        const results = await Promise.all(batch.map(file => uploadFile(s3Client, file)));
        const batchSuccessCount = results.filter(r => r.success).length;
        successCount += batchSuccessCount;
        
        if(batchSuccessCount < batch.length) {
             console.log(`⚠️ Batch này có ${batch.length - batchSuccessCount} files bị lỗi!`);
        } else {
             console.log(`✅ ${batchSuccessCount} files thành công.`);
        }

        // Nghỉ 500ms giữa các luồng để tránh Cloudflare block Request
        await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`\n🎉 Hoàn thành! Đã upload thành công ${successCount}/${allFiles.length} files.`);
}

startUpload().catch(console.error);
