/**
 * Grid Controller — serves full-resolution grid data.
 *
 * ?format=bin  → binary (4-byte header + JSON metadata + Float32 data)
 *                ~70% faster than JSON for 2M-cell grids.
 * ?format=json → standard JSON envelope (fast-json-stringify for speed)
 */
import { Router } from 'express';
import FastJson from 'fast-json-stringify';
import { GridParamsSchema } from './grid.types';
import * as gridService from './grid.service';
import { fail } from '../../shared/types/envelope';
import crypto from 'crypto';

const router = Router();

function computeGridBinEtag(metaBuf: Buffer, dataBuf: Buffer): string {
    // Hash small prefix + lengths to keep CPU bounded (dataBuf can be multi-MB).
    const prefixLen = Math.min(64 * 1024, dataBuf.length);
    const h = crypto.createHash('sha1');
    h.update(metaBuf);
    h.update('|');
    h.update(String(metaBuf.length));
    h.update('|');
    h.update(String(dataBuf.length));
    h.update('|');
    h.update(dataBuf.subarray(0, prefixLen));
    return `W/"gridbin-${h.digest('base64url')}"`;
}

const stringifyGridResponse = FastJson({
    type: 'object',
    properties: {
        success: { type: 'boolean' },
        data: {
            type: 'object',
            properties: {
                v: { type: 'integer' },
                region: { type: 'string' },
                date: { type: 'string' },
                layer: { type: 'string' },
                bounds: {
                    type: 'object',
                    properties: {
                        n: { type: 'number' },
                        s: { type: 'number' },
                        e: { type: 'number' },
                        w: { type: 'number' },
                    },
                },
                size: {
                    type: 'object',
                    properties: {
                        r: { type: 'integer' },
                        c: { type: 'integer' },
                    },
                },
                scale: { type: 'number' },
                nodata: { type: 'number' },
                data: { type: 'array', items: { type: 'number' } },
            },
            required: ['v', 'region', 'date', 'layer', 'bounds', 'size', 'scale', 'nodata', 'data'],
        },
    },
    required: ['success', 'data'],
} as const);

router.get('/grid/:region/:date/:layer', async (req, res) => {
    const parsed = GridParamsSchema.safeParse(req.params);
    if (!parsed.success) {
        const issue = parsed.error.issues[0];
        return fail(res, `${issue?.path.join('.')}: ${issue?.message}`, 400, 'VALIDATION');
    }

    const result = await gridService.getGrid(parsed.data);
    if (!result.ok) return fail(res, result.error.message, result.error.statusCode, result.error.code);

    const grid = result.value;
    const format = (req.query.format as string) || 'bin';

    if (format === 'bin') {
        const meta = JSON.stringify({
            v: grid.v, region: grid.region, date: grid.date, layer: grid.layer,
            bounds: grid.bounds, size: grid.size, scale: grid.scale, nodata: grid.nodata,
        });
        const metaBuf = Buffer.from(meta, 'utf-8');

        const headerBuf = Buffer.alloc(4);
        headerBuf.writeUInt32LE(metaBuf.length, 0);

        let dataBuf: Buffer;
        if (grid.data instanceof Float32Array) {
            dataBuf = Buffer.from(grid.data.buffer, grid.data.byteOffset, grid.data.byteLength);
        } else {
            const f32 = new Float32Array(grid.data.length);
            for (let i = 0; i < grid.data.length; i++) f32[i] = grid.data[i]!;
            dataBuf = Buffer.from(f32.buffer, f32.byteOffset, f32.byteLength);
        }

        const etag = computeGridBinEtag(metaBuf, dataBuf);
        if (req.headers['if-none-match'] === etag) {
            res.setHeader('ETag', etag);
            res.setHeader('Cache-Control', 'public, max-age=1800, stale-while-revalidate=600');
            return res.status(304).end();
        }

        const body = Buffer.concat([headerBuf, metaBuf, dataBuf]);
        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('Content-Length', String(body.length));
        res.setHeader('Cache-Control', 'public, max-age=1800, stale-while-revalidate=600');
        res.setHeader('ETag', etag);
        return res.status(200).end(body);
    }

    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'public, max-age=1800, stale-while-revalidate=600');
    const forJson = grid.data instanceof Float32Array
        ? { ...grid, data: Array.from(grid.data) }
        : grid;
    return res.status(200).send(stringifyGridResponse({ success: true, data: forJson }));
});

export { router as gridRouter };
