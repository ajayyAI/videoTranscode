import express from 'express';
import { generateSignedUrl } from '../utils/cloudfront-url';

const router = express.Router();

interface GenerateUrlRequest {
    videoKey: string;
    expiresIn?: number;
}

// POST /api/url/generate
router.post('/generate', (req, res) => {
    try {
        const { videoKey, expiresIn }: GenerateUrlRequest = req.body;

        if (!videoKey) {
            return res.status(400).json({
                error: 'videoKey is required'
            });
        }

        const signedUrl = generateSignedUrl({
            videoKey,
            expiresIn
        });

        return res.json({
            url: signedUrl
        });
    } catch (error) {
        console.error('Error generating signed URL:', error);
        return res.status(500).json({
            error: 'Failed to generate signed URL'
        });
    }
});

export default router;
