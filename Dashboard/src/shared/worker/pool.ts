/**
 * TIF Worker Pool — Manages a pool of worker threads for CPU-bound TIF decode.
 *
 * Architecture:
 *   Main thread → pool.decode(buffer, col, row) → idle worker → result
 *   If no idle worker → queue task (backpressure if queue full → reject)
 *
 * Features:
 *   - Pool size: os.cpus() - 1 (reserve 1 for event loop)
 *   - Backpressure: max queue depth 50, returns 503 when full
 *   - Task timeout: 5s per decode
 *   - Graceful shutdown: drain queue, terminate workers
 */
import { Worker } from 'worker_threads';
import os from 'os';
import path from 'path';
import { structuredLog } from '../middleware/tracing';
import type { DecodeRequest, DecodeResponse } from './protocol';

interface PendingTask {
    resolve: (values: (number | null)[]) => void;
    reject: (err: Error) => void;
    msg: DecodeRequest;
    timer: ReturnType<typeof setTimeout>;
}

export class TifWorkerPool {
    private workers: Worker[] = [];
    private queue: PendingTask[] = [];
    private busy = new Map<Worker, PendingTask>();
    private readonly maxQueue: number;
    private readonly taskTimeoutMs: number;
    private _totalDecoded = 0;

    constructor(opts: { maxWorkers?: number; maxQueueSize?: number; taskTimeoutMs?: number } = {}) {
        const size = opts.maxWorkers ?? Math.max(1, os.cpus().length - 1);
        this.maxQueue = opts.maxQueueSize ?? 50;
        this.taskTimeoutMs = opts.taskTimeoutMs ?? 5000;
        const ext = __filename.endsWith('.ts') ? 'ts' : 'js';
        const workerPath = path.join(__dirname, `tif-worker.${ext}`);
        for (let i = 0; i < size; i++) {
            const workerOps: Record<string, any> = { workerData: {} };
            if (ext === 'ts') workerOps.execArgv = ['--import', 'tsx'];
            const w = new Worker(workerPath, workerOps);
            w.on('message', (result: DecodeResponse) => this.onResult(w, result));
            w.on('error', (err: Error) => this.onError(w, err));
            this.workers.push(w);
        }

        structuredLog('info', 'worker_pool_init', { poolSize: size, maxQueue: this.maxQueue });
    }

    async decode(buffer: ArrayBuffer, col: number, row: number, bandCount = 8): Promise<(number | null)[]> {
        if (this.queue.length >= this.maxQueue) {
            throw new Error('Worker pool queue full (backpressure)');
        }

        return new Promise<(number | null)[]>((resolve, reject) => {
            const id = `task_${++this._totalDecoded}`;
            const timer = setTimeout(() => {
                reject(new Error(`Worker decode timeout after ${this.taskTimeoutMs}ms`));
            }, this.taskTimeoutMs);

            const task: PendingTask = {
                resolve: (v) => { clearTimeout(timer); resolve(v); },
                reject: (e) => { clearTimeout(timer); reject(e); },
                msg: { id, buffer, col, row, bandCount },
                timer,
            };

            // Find idle worker
            const idle = this.workers.find(w => !this.busy.has(w));
            if (idle) {
                this.dispatch(idle, task);
            } else {
                this.queue.push(task);
            }
        });
    }

    private dispatch(worker: Worker, task: PendingTask): void {
        this.busy.set(worker, task);
        worker.postMessage(task.msg);
    }

    private onResult(worker: Worker, result: DecodeResponse): void {
        const task = this.busy.get(worker);
        this.busy.delete(worker);

        if (task) {
            if (result.error) {
                task.reject(new Error(result.error));
            } else {
                task.resolve(result.values);
            }
        }

        // Process next in queue
        if (this.queue.length > 0) {
            const next = this.queue.shift()!;
            this.dispatch(worker, next);
        }
    }

    private onError(worker: Worker, err: Error): void {
        const task = this.busy.get(worker);
        this.busy.delete(worker);
        if (task) {
            task.reject(err);
        }
        structuredLog('error', 'worker_error', { error: err.message });

        // Replace crashed worker
        const idx = this.workers.indexOf(worker);
        if (idx >= 0) {
            const ext = __filename.endsWith('.ts') ? 'ts' : 'js';
            const workerPath = path.join(__dirname, `tif-worker.${ext}`);
            const workerOps: Record<string, any> = { workerData: {} };
            if (ext === 'ts') workerOps.execArgv = ['--import', 'tsx'];
            const newW = new Worker(workerPath, workerOps);
            newW.on('message', (r: DecodeResponse) => this.onResult(newW, r));
            newW.on('error', (e: Error) => this.onError(newW, e));
            this.workers[idx] = newW;
        }
    }

    get stats() {
        return {
            poolSize: this.workers.length,
            busy: this.busy.size,
            queued: this.queue.length,
            totalDecoded: this._totalDecoded,
        };
    }

    async shutdown(): Promise<void> {
        // Clear queue
        for (const task of this.queue) {
            clearTimeout(task.timer);
            task.reject(new Error('Pool shutting down'));
        }
        this.queue = [];

        // Terminate workers
        await Promise.all(this.workers.map(w => w.terminate()));
        structuredLog('info', 'worker_pool_shutdown', { totalDecoded: this._totalDecoded });
    }
}
