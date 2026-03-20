/**
 * Standard API Response Envelope — Every response uses this shape.
 */
import type { Response } from 'express';
import { structuredLog } from '../middleware/tracing';

export interface ApiResponse<T> {
    success: boolean;
    data?: T;
    error?: { code: string; message: string };
}

export function ok<T>(res: Response, data: T, status = 200): Response {
    return res.status(status).json({ success: true, data } satisfies ApiResponse<T>);
}

export function fail(
    res: Response,
    message: string,
    status = 500,
    code?: string,
): Response {
    structuredLog('error', 'api_error', { statusCode: status, code: code ?? String(status), message });
    return res.status(status).json({
        success: false,
        error: { code: code ?? String(status), message },
    } satisfies ApiResponse<never>);
}
