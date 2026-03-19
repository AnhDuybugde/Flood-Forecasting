/**
 * Worker Thread message protocol — typed request/response contract.
 */

export interface DecodeRequest {
    id: string;
    buffer: ArrayBuffer;
    col: number;
    row: number;
    bandCount: number;
}

export interface DecodeResponse {
    id: string;
    values: (number | null)[];
    decodeMs: number;
    error?: string;
}
