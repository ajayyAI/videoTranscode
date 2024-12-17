import express from 'express';
import { VideoService } from '../services/video.service';
import { GenerateUrlRequest, StreamRequest, VideoQuality } from '../types/video';

const router = express.Router();

/**
 * Generate signed URL for video assets
 * POST /api/url/generate
 */
router.post('/generate', (req, res) => {
    const { videoKey, expiresIn }: GenerateUrlRequest = req.body;

    if (!videoKey) {
        return res.status(400).json({
            success: false,
            error: 'videoKey is required'
        });
    }

    const result = VideoService.generateUrl({ videoKey, expiresIn });
    return res.status(result.success ? 200 : 500).json(result);
});

/**
 * Get HLS streaming URLs for a video
 * GET /api/url/stream/:courseId/:videoId
 */
router.get('/stream/:courseId/:videoId', (req, res) => {
    const { courseId, videoId } = req.params;
    const rawQuality = req.query.quality as string | undefined;

    if (!courseId || !videoId) {
        return res.status(400).json({
            success: false,
            error: 'courseId and videoId are required'
        });
    }

    // Validate quality parameter if provided
    const quality = rawQuality as VideoQuality | undefined;
    if (rawQuality && !['360p', '480p', '720p', '1080p'].includes(rawQuality)) {
        return res.status(400).json({
            success: false,
            error: 'Invalid quality value. Must be one of: 360p, 480p, 720p, 1080p'
        });
    }

    const result = VideoService.generateStreamUrls({ courseId, videoId, quality });
    return res.status(result.success ? 200 : 500).json(result);
});

/**
 * Test HLS playback page
 * GET /api/url/player/:courseId/:videoId
 */
router.get('/player/:courseId/:videoId', (req, res) => {
    const { courseId, videoId } = req.params;
    
    if (!courseId || !videoId) {
        return res.status(400).json({
            success: false,
            error: 'courseId and videoId are required'
        });
    }

    try {
        const html = VideoService.generatePlayerHtml(courseId, videoId);
        res.setHeader('Content-Type', 'text/html');
        return res.send(html);
    } catch (error) {
        console.error('Error generating player page:', error);
        return res.status(500).json({
            success: false,
            error: 'Failed to generate player page'
        });
    }
});

export default router;
