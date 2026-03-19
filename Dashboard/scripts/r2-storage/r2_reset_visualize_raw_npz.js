const fs = require('fs');
const path = require('path');
const https = require('https');
const {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  PutObjectCommand,
} = require('@aws-sdk/client-s3');
require('dotenv').config();

const BUCKET = process.env.R2_BUCKET_NAME;
const ACCOUNT_ID = process.env.R2_ACCOUNT_ID;
const ACCESS_KEY = process.env.R2_ACCESS_KEY_ID;
const SECRET_KEY = process.env.R2_SECRET_ACCESS_KEY;

if (!BUCKET || !ACCOUNT_ID || !ACCESS_KEY || !SECRET_KEY) {
  console.error('Missing R2 env vars. Ensure .env has R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME');
  process.exit(1);
}

// What the user sees in Cloudflare UI: prefix=visualize/
const DELETE_PREFIX = 'visualize/';
// Upload target: visualize/2020-2025/Data_Training_Raw_NPZ/<file>.npz
const UPLOAD_PREFIX = 'visualize/2020-2025/Data_Training_Raw_NPZ/';
const LOCAL_DIR = path.resolve(__dirname, '..', 'data', 'visualize', 'Data_Training_Raw_NPZ');
const SKIP_DELETE = process.env.SKIP_DELETE === '1';

async function getServerTime() {
  return new Promise((resolve, reject) => {
    https
      .get('https://cloudflare.com', (res) => {
        const dateStr = res.headers.date;
        resolve(dateStr ? new Date(dateStr) : new Date());
      })
      .on('error', reject);
  });
}

function createClientWithClockOffset(serverTime) {
  const offset = serverTime.getTime() - Date.now();
  console.log(`Using AWS SDK systemClockOffset: ${offset}ms`);
  return new S3Client({
    region: 'auto',
    endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: ACCESS_KEY, secretAccessKey: SECRET_KEY },
    systemClockOffset: offset,
    maxAttempts: 3,
  });
}

async function listAllKeys(s3, prefix) {
  const keys = [];
  let token = undefined;
  do {
    const resp = await s3.send(
      new ListObjectsV2Command({
        Bucket: BUCKET,
        Prefix: prefix,
        ContinuationToken: token,
        MaxKeys: 1000,
      }),
    );
    if (resp.Contents) {
      for (const obj of resp.Contents) {
        if (obj.Key) keys.push(obj.Key);
      }
    }
    token = resp.NextContinuationToken;
  } while (token);
  return keys;
}

async function deleteKeys(s3, keys) {
  const BATCH = 1000; // S3 delete batch limit
  let deleted = 0;
  for (let i = 0; i < keys.length; i += BATCH) {
    const chunk = keys.slice(i, i + BATCH);
    await s3.send(
      new DeleteObjectsCommand({
        Bucket: BUCKET,
        Delete: { Objects: chunk.map((Key) => ({ Key })), Quiet: true },
      }),
    );
    deleted += chunk.length;
    console.log(`Deleted ${deleted}/${keys.length}`);
  }
}

async function getLocalFiles(dir) {
  const out = [];
  const items = await fs.promises.readdir(dir, { withFileTypes: true });
  for (const it of items) {
    if (it.name === '.DS_Store') continue;
    const full = path.join(dir, it.name);
    if (it.isDirectory()) out.push(...(await getLocalFiles(full)));
    else out.push(full);
  }
  return out;
}

async function uploadFile(s3, filePath) {
  const body = fs.readFileSync(filePath);
  const filename = path.basename(filePath);
  const key = `${UPLOAD_PREFIX}${filename}`;

  const MAX_RETRIES = 4;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      await s3.send(new PutObjectCommand({ Bucket: BUCKET, Key: key, Body: body }));
      return key;
    } catch (e) {
      const msg = (e && e.message) || String(e);
      const retryable = /SlowDown|Throttl|Timeout|ECONNRESET|socket|RequestTimeout|503|504/i.test(msg);
      if (attempt < MAX_RETRIES && retryable) {
        const sleep = 500 * Math.pow(2, attempt);
        console.warn(`Retrying upload (${attempt + 1}/${MAX_RETRIES}) for ${key} after ${sleep}ms: ${msg}`);
        await new Promise((r) => setTimeout(r, sleep));
        continue;
      }
      throw e;
    }
  }
  return key;
}

async function main() {
  if (!fs.existsSync(LOCAL_DIR)) {
    console.error(`Local dir not found: ${LOCAL_DIR}`);
    process.exit(1);
  }

  console.log(`Bucket: ${BUCKET}`);
  console.log(`DELETE_PREFIX: ${DELETE_PREFIX}`);
  console.log(`LOCAL_DIR: ${LOCAL_DIR}`);
  console.log(`UPLOAD_PREFIX: ${UPLOAD_PREFIX}`);

  const serverTime = await getServerTime();
  const s3 = createClientWithClockOffset(serverTime);

  // 1) Delete existing data under visualize/
  if (!SKIP_DELETE) {
    console.log('\nListing remote keys to delete...');
    const keys = await listAllKeys(s3, DELETE_PREFIX);
    console.log(`Found ${keys.length} keys under ${DELETE_PREFIX}`);
    if (keys.length > 0) {
      console.log('Deleting...');
      await deleteKeys(s3, keys);
      console.log('Delete complete.');
    } else {
      console.log('Nothing to delete.');
    }
  } else {
    console.log('\nSKIP_DELETE=1 → skipping delete step.');
  }

  // 2) Upload new raw NPZ
  console.log('\nScanning local files...');
  const files = await getLocalFiles(LOCAL_DIR);
  console.log(`Found ${files.length} local files.`);

  // Skip already-uploaded keys (saves time)
  console.log('\nListing existing uploaded keys...');
  const existingKeys = new Set(await listAllKeys(s3, UPLOAD_PREFIX));
  console.log(`Existing: ${existingKeys.size} keys under ${UPLOAD_PREFIX}`);

  const CONCURRENCY = 4;
  let idx = 0;
  let ok = 0;
  let skipped = 0;
  const failed = [];
  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = idx++;
      if (i >= files.length) return;
      const fp = files[i];
      try {
        const filename = path.basename(fp);
        const key = `${UPLOAD_PREFIX}${filename}`;
        if (existingKeys.has(key)) {
          skipped++;
          continue;
        }

        await uploadFile(s3, fp);
        ok++;
        if (ok % 10 === 0 || ok === files.length) {
          console.log(`Uploaded ${ok}/${files.length} (latest: ${key})`);
        }
      } catch (e) {
        const msg = (e && e.message) || String(e);
        failed.push({ file: fp, error: msg });
        console.error(`Upload failed (skipping): ${fp} -> ${msg}`);
      }
    }
  });

  await Promise.all(workers);
  console.log(`\nDone. Uploaded ${ok} new files, skipped ${skipped} existing, total local ${files.length}.`);
  if (failed.length) {
    const outPath = path.resolve(process.cwd(), 'r2_upload_failed.json');
    fs.writeFileSync(outPath, JSON.stringify(failed, null, 2));
    console.log(`Failed ${failed.length} files. Saved list to ${outPath}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

