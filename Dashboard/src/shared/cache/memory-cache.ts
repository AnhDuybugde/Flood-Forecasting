/**
 * Cache interface — Dependency Inversion for L1/L2 swap.
 */

export interface ICacheProvider<T = unknown> {
    get(key: string): T | undefined;
    set(key: string, value: T, ttlMs?: number): void;
    has(key: string): boolean;
    delete(key: string): boolean;
    clear(): void;
    readonly size: number;
}

/**
 * In-memory LRU cache with TTL support.
 * Used as L1 cache for TIF images and pixel results.
 */
export class MemoryCache<T = unknown> implements ICacheProvider<T> {
    private cache = new Map<string, { value: T; expiresAt: number }>();
    private readonly maxSize: number;
    private readonly defaultTtlMs: number;

    constructor(maxSize = 500, defaultTtlMs = 60 * 60 * 1000) {
        this.maxSize = maxSize;
        this.defaultTtlMs = defaultTtlMs;
    }

    get(key: string): T | undefined {
        const entry = this.cache.get(key);
        if (!entry) return undefined;
        if (Date.now() > entry.expiresAt) {
            this.cache.delete(key);
            return undefined;
        }
        // LRU: move to end
        this.cache.delete(key);
        this.cache.set(key, entry);
        return entry.value;
    }

    set(key: string, value: T, ttlMs?: number): void {
        // Evict oldest if at capacity
        if (this.cache.size >= this.maxSize && !this.cache.has(key)) {
            const oldest = this.cache.keys().next().value;
            if (oldest !== undefined) this.cache.delete(oldest);
        }
        this.cache.set(key, {
            value,
            expiresAt: Date.now() + (ttlMs ?? this.defaultTtlMs),
        });
    }

    has(key: string): boolean {
        const entry = this.cache.get(key);
        if (!entry) return false;
        if (Date.now() > entry.expiresAt) {
            this.cache.delete(key);
            return false;
        }
        return true;
    }

    delete(key: string): boolean {
        return this.cache.delete(key);
    }

    clear(): void {
        this.cache.clear();
    }

    get size(): number {
        return this.cache.size;
    }
}
