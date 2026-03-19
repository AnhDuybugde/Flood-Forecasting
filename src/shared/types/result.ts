/**
 * Result<T, E> — Discriminated union for error handling.
 * NEVER throw inside controllers. Always return Result.
 */

export interface AppError {
    code: string;
    message: string;
    statusCode: number;
}

export type Result<T, E = AppError> =
    | { ok: true; value: T }
    | { ok: false; error: E };

export const Ok = <T>(value: T): Result<T, never> => ({ ok: true, value });

export const Err = <E extends AppError>(error: E): Result<never, E> => ({
    ok: false,
    error,
});

/** Convenience: create AppError from common patterns */
export const AppErrors = {
    validation: (message: string): AppError => ({
        code: 'VALIDATION',
        message,
        statusCode: 400,
    }),
    notFound: (message: string): AppError => ({
        code: 'NOT_FOUND',
        message,
        statusCode: 404,
    }),
    outOfBounds: (message: string): AppError => ({
        code: 'OUT_OF_BOUNDS',
        message,
        statusCode: 404,
    }),
    internal: (message: string): AppError => ({
        code: 'INTERNAL',
        message,
        statusCode: 500,
    }),
    timeout: (message: string): AppError => ({
        code: 'TIMEOUT',
        message,
        statusCode: 504,
    }),
    backpressure: (): AppError => ({
        code: 'BACKPRESSURE',
        message: 'Server is overloaded, please retry',
        statusCode: 503,
    }),
} as const;
