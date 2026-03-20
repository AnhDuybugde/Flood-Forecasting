/**
 * Structured logging + trace ID middleware.
 * ALL logs are JSON for production observability.
 */

let _requestCounter = 0;

export function nextTraceId(prefix = 'req'): string {
    return `${prefix}_${++_requestCounter}`;
}

export function structuredLog(
    level: 'info' | 'warn' | 'error',
    event: string,
    data: Record<string, unknown> = {},
): void {
    const entry = {
        timestamp: new Date().toISOString(),
        level,
        event,
        ...data,
    };
    const json = JSON.stringify(entry);
    if (level === 'error') console.error(json);
    else if (level === 'warn') console.warn(json);
    else console.log(json);
}
