/**
 * Zod-validated environment configuration.
 * Loads .env defensively inside loadEnv() — TypeScript hoists static imports
 * above runtime code, so dotenv.config() in index.ts runs AFTER app.ts
 * and its transitive dependencies already call loadEnv().
 */
import { z } from 'zod';
import dotenv from 'dotenv';
import path from 'path';

const EnvSchema = z.object({
    PORT: z.coerce.number().default(8000),
    NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
    R2_ACCOUNT_ID: z.string().optional(),
    R2_ACCESS_KEY_ID: z.string().optional(),
    R2_SECRET_ACCESS_KEY: z.string().optional(),
    R2_BUCKET_NAME: z.string().default('satellite-data-10x10'),
    R2_PUBLIC_URL: z.string().optional(),
    WORKER_POOL_SIZE: z.coerce.number().min(1).max(16).optional(),
    USE_WORKER_POOL: z.coerce.boolean().default(false),
    USE_PREBUILT_GRID: z.coerce.boolean().default(false),
    INFERENCE_API_URL: z.string().default('http://localhost:8001'),
    INFERENCE_TIMEOUT_MS: z.coerce.number().default(5000),
});

export type Env = z.infer<typeof EnvSchema>;

let _env: Env | null = null;
let _dotenvLoaded = false;

function ensureDotenv(): void {
    if (_dotenvLoaded) return;
    _dotenvLoaded = true;
    // Walk up from src/shared/config/ (or dist/shared/config/) to project root
    let dir = __dirname;
    for (let i = 0; i < 5; i++) {
        const candidate = path.join(dir, '.env');
        const result = dotenv.config({ path: candidate });
        if (!result.error) break;
        dir = path.dirname(dir);
    }
}

export function loadEnv(): Env {
    if (_env) return _env;
    ensureDotenv();
    const result = EnvSchema.safeParse(process.env);
    if (!result.success) {
        console.error('❌ Environment validation failed:');
        console.error(result.error.format());
        _env = EnvSchema.parse({});
        return _env;
    }
    _env = result.data;
    return _env;
}

export function getEnv(): Env {
    if (!_env) return loadEnv();
    return _env;
}
