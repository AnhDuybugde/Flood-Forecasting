/**
 * Metadata Controller — Date index + timeline endpoints.
 */
import { Router } from 'express';
import { MetadataParamsSchema } from './metadata.types';
import * as metadataService from './metadata.service';
import { ok, fail } from '../../shared/types/envelope';
import { VALID_REGIONS } from '../../shared/types/common';

const router = Router();

// GET /api/dates/:region
router.get('/dates/:region', async (req, res) => {
    const parsed = MetadataParamsSchema.safeParse(req.params);
    if (!parsed.success) {
        return fail(res, `Invalid region. Valid: ${VALID_REGIONS.join(', ')}`, 400, 'VALIDATION');
    }

    const result = await metadataService.getDates(parsed.data.region);
    if (!result.ok) return fail(res, result.error.message, result.error.statusCode, result.error.code);

    res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=300');
    return ok(res, result.value);
});

// GET /api/timeline
router.get('/timeline', async (_req, res) => {
    const result = await metadataService.getTimeline();
    if (!result.ok) return fail(res, result.error.message, result.error.statusCode, result.error.code);

    res.setHeader('Cache-Control', 'public, max-age=600, stale-while-revalidate=300');
    return ok(res, result.value);
});

export { router as metadataRouter };
