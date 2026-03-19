/**
 * TIF Worker — Runs in a Worker Thread to decode GeoTIFF raster data.
 * Isolates CPU-bound readRasters() from the main event loop.
 */
import { parentPort } from 'worker_threads';
import { fromArrayBuffer } from 'geotiff';
import type { DecodeRequest, DecodeResponse } from './protocol';

if (!parentPort) {
    throw new Error('tif-worker.ts must run inside a Worker Thread');
}

parentPort.on('message', async (msg: DecodeRequest) => {
    const t0 = performance.now();
    try {
        const tif = await fromArrayBuffer(msg.buffer);
        const image = await tif.getImage();
        const rasters = await image.readRasters({
            window: [msg.col, msg.row, msg.col + 1, msg.row + 1],
        });

        const nodataStr = (image.fileDirectory as any)?.GDAL_NODATA;
        const nod = nodataStr !== undefined ? parseFloat(nodataStr) : -9999;

        const values: (number | null)[] = [];
        for (let i = 0; i < msg.bandCount; i++) {
            const band = rasters[i];
            const raw = band ? (band as Float64Array)[0] : null;
            if (raw === null || raw === undefined || raw === nod || isNaN(raw) || raw <= -9998) {
                values.push(null);
            } else {
                values.push(raw);
            }
        }

        const response: DecodeResponse = {
            id: msg.id,
            values,
            decodeMs: Math.round(performance.now() - t0),
        };
        parentPort!.postMessage(response);
    } catch (err) {
        const response: DecodeResponse = {
            id: msg.id,
            values: [],
            decodeMs: Math.round(performance.now() - t0),
            error: (err as Error).message,
        };
        parentPort!.postMessage(response);
    }
});
