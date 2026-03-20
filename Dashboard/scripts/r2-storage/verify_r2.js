const fs = require('fs');
const path = require('path');
const { S3Client, ListObjectsV2Command } = require('@aws-sdk/client-s3');
require('dotenv').config();

const bucketName = process.env.R2_BUCKET_NAME;
const sourceDataDir = path.join(__dirname, '../data');

const s3Client = new S3Client({
    region: 'auto',
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY_ID,
        secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
    }
});

async function getLocalFiles(dir) {
    let files = [];
    const items = await fs.promises.readdir(dir, { withFileTypes: true });

    for (const item of items) {
        if (item.name === '.DS_Store') continue;

        const fullPath = path.join(dir, item.name);
        if (item.isDirectory()) {
            files = files.concat(await getLocalFiles(fullPath));
        } else {
            // Lấy relative path giống với key trên R2
            const relativePath = path.relative(sourceDataDir, fullPath).split(path.sep).join('/');
            files.push(relativePath);
        }
    }
    return files;
}

async function getR2Files() {
    let uploadedFiles = new Set();
    let continuationToken = undefined;

    console.log("Đang lấy danh sách các file trên R2 bucket...");
    do {
        const listCommand = new ListObjectsV2Command({
            Bucket: bucketName,
            ContinuationToken: continuationToken,
        });

        const response = await s3Client.send(listCommand);

        if (response.Contents) {
            response.Contents.forEach(obj => uploadedFiles.add(obj.Key));
        }

        continuationToken = response.NextContinuationToken;
    } while (continuationToken);

    return uploadedFiles;
}

async function verifyUpload() {
    console.log("Bắt đầu đối chiếu dữ liệu...");
    
    // 1. Lấy danh sách file local
    const localFiles = await getLocalFiles(sourceDataDir);
    console.log(`\n📌 Local: Tìm thấy ${localFiles.length} files trong thư mục data.`);

    // 2. Lấy danh sách file trên R2
    const r2Files = await getR2Files();
    console.log(`📌 R2 Bucket: Tìm thấy ${r2Files.size} files hiện có trên cloud.`);

    // 3. So sánh
    const missingFiles = localFiles.filter(file => !r2Files.has(file));

    console.log(`\n================ KẾT QUẢ KIỂM TRA ================`);
    if (missingFiles.length === 0) {
        console.log(`✅ Tuyệt vời! Tất cả ${localFiles.length} files đã được tải lên đầy đủ, KHÔNG BỎ SÓT FILE NÀO.`);
    } else {
        console.log(`❌ Phát hiện ${missingFiles.length} files bị thiếu (chưa lên Cloudflare):`);
        missingFiles.forEach(file => console.log(` - ${file}`));
        
        // Ghi log ra file để dễ nhìn
        fs.writeFileSync('missing_files.json', JSON.stringify(missingFiles, null, 2));
        console.log(`\n👉 Đã lưu danh sách các file lỗi vào 'missing_files.json'.`);
    }
}

verifyUpload().catch(console.error);
