/**
 * Diagnostic: List actual R2 bucket structure using Node.js AWS SDK.
 * Run: node scripts/diag_r2_list.mjs
 */
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { config } from 'dotenv';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
config({ path: resolve(__dirname, '..', '.env') });

const { R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME } = process.env;
const bucket = R2_BUCKET_NAME || 'satellite-data-10x10';

if (!R2_ACCOUNT_ID || !R2_ACCESS_KEY_ID || !R2_SECRET_ACCESS_KEY) {
    console.error('ERROR: R2 credentials not found in .env');
    process.exit(1);
}

const s3 = new S3Client({
    region: 'auto',
    endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
});

async function listPrefix(prefix, maxKeys = 20) {
    try {
        const resp = await s3.send(new ListObjectsV2Command({
            Bucket: bucket, Prefix: prefix, MaxKeys: maxKeys,
        }));
        return (resp.Contents || []).map(o => ({ key: o.Key, size: o.Size }));
    } catch (e) {
        return [{ error: e.message || e.Code }];
    }
}

async function listDelimited(prefix) {
    try {
        const resp = await s3.send(new ListObjectsV2Command({
            Bucket: bucket, Prefix: prefix, Delimiter: '/', MaxKeys: 100,
        }));
        return {
            prefixes: (resp.CommonPrefixes || []).map(p => p.Prefix),
            files: (resp.Contents || []).map(o => ({ key: o.Key, size: o.Size })),
        };
    } catch (e) {
        return { error: e.message || e.Code };
    }
}

console.log(`Bucket: ${bucket}`);
console.log('='.repeat(80));

// 1) Top-level
console.log('\n--- TOP-LEVEL ---');
const top = await listDelimited('');
console.log(JSON.stringify(top, null, 2));

// 2) FloodData/ subfolders
console.log('\n--- FloodData/ subfolders ---');
const fd = await listDelimited('FloodData/');
console.log(JSON.stringify(fd, null, 2));

// 3) DaNang subfolders
console.log('\n--- FloodData/DaNang/ subfolders ---');
const dn = await listDelimited('FloodData/DaNang/');
console.log(JSON.stringify(dn, null, 2));

// 4) Stacked folder
console.log('\n--- FloodData/DaNang/Stacked/ (first 10) ---');
const stacked = await listPrefix('FloodData/DaNang/Stacked/', 10);
console.log(JSON.stringify(stacked, null, 2));

// 5) Daily folder
console.log('\n--- FloodData/DaNang/Daily/ subfolders ---');
const daily = await listDelimited('FloodData/DaNang/Daily/');
console.log(JSON.stringify(daily, null, 2));

// 6) Rain files for 2020-01
console.log('\n--- FloodData/DaNang/Daily/Rain/ (first 10) ---');
const rain = await listPrefix('FloodData/DaNang/Daily/Rain/', 10);
console.log(JSON.stringify(rain, null, 2));

// 7) Static files
console.log('\n--- FloodData/DaNang/Static/ ---');
const stat = await listPrefix('FloodData/DaNang/Static/', 20);
console.log(JSON.stringify(stat, null, 2));

// 8) LabelDaily
console.log('\n--- FloodData/DaNang/LabelDaily/ (first 10) ---');
const label = await listPrefix('FloodData/DaNang/LabelDaily/', 10);
console.log(JSON.stringify(label, null, 2));

// 9) Check alternative casing
for (const alt of ['FloodData/danang/', 'flooddata/', 'Flooddata/', 'flood_data/']) {
    const r = await listPrefix(alt, 3);
    if (r.length > 0 && !r[0].error) {
        console.log(`\n--- FOUND: ${alt} ---`);
        console.log(JSON.stringify(r, null, 2));
    }
}

// 10) Total bucket size
console.log('\n--- BUCKET TOTAL (first page) ---');
const all = await listPrefix('', 5);
console.log(JSON.stringify(all, null, 2));
