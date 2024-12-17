import { generateSignedUrl } from '../utils/cloudfront-url';
import { GenerateUrlRequest, StreamRequest, VideoResponse, VideoQuality } from '../types/video';

export class VideoService {
    private static readonly DEFAULT_EXPIRES_IN = 3600; // 1 hour
    private static readonly SUPPORTED_QUALITIES: VideoQuality[] = ['360p', '480p', '720p', '1080p'];

    /**
     * Generate a signed URL for a video asset
     */
    public static generateUrl(params: GenerateUrlRequest): VideoResponse {
        const { videoKey, expiresIn = this.DEFAULT_EXPIRES_IN } = params;

        if (!this.isValidKey(videoKey)) {
            return {
                success: false,
                error: 'Invalid videoKey format'
            };
        }

        try {
            const signedUrl = generateSignedUrl({ videoKey, expiresIn });
            return {
                success: true,
                data: {
                    url: signedUrl,
                    expiresIn,
                    expiresAt: this.getExpiryDate(expiresIn)
                }
            };
        } catch (error) {
            console.error('Error generating signed URL:', error);
            return {
                success: false,
                error: 'Failed to generate signed URL'
            };
        }
    }

    /**
     * Generate streaming URLs for HLS video
     */
    public static generateStreamUrls(params: StreamRequest): VideoResponse {
        const { courseId, videoId, quality } = params;

        if (!this.isValidKey(courseId) || !this.isValidKey(videoId)) {
            return {
                success: false,
                error: 'Invalid courseId or videoId format'
            };
        }

        try {
            const baseKey = `${courseId}/hls/${videoId}`;
            const expiresIn = this.DEFAULT_EXPIRES_IN;

            const masterPlaylistUrl = generateSignedUrl({
                videoKey: `${baseKey}/master.m3u8`,
                expiresIn
            });

            const response: VideoResponse = {
                success: true,
                data: {
                    masterPlaylist: masterPlaylistUrl,
                    expiresIn,
                    expiresAt: this.getExpiryDate(expiresIn)
                }
            };

            if (quality && this.SUPPORTED_QUALITIES.includes(quality)) {
                response.data!.qualityPlaylist = generateSignedUrl({
                    videoKey: `${baseKey}/${quality}/playlist.m3u8`,
                    expiresIn
                });
                response.data!.quality = quality;
            }

            return response;
        } catch (error) {
            console.error('Error generating streaming URLs:', error);
            return {
                success: false,
                error: 'Failed to generate streaming URLs'
            };
        }
    }

    /**
     * Generate HTML for video player
     */
    public static generatePlayerHtml(courseId: string, videoId: string): string {
        const baseKey = `${courseId}/hls/${videoId}`;
        const masterPlaylistUrl = generateSignedUrl({
            videoKey: `${baseKey}/master.m3u8`,
            expiresIn: this.DEFAULT_EXPIRES_IN
        });

        return `
<!DOCTYPE html>
<html>
<head>
    <title>HLS Player - ${courseId}/${videoId}</title>
    <link href="https://vjs.zencdn.net/8.6.1/video-js.css" rel="stylesheet" />
    <script src="https://vjs.zencdn.net/8.6.1/video.min.js"></script>
    <style>
        body { margin: 0; background: #000; }
        .video-container { 
            width: 100vw; 
            height: 100vh; 
            display: flex; 
            justify-content: center; 
            align-items: center; 
        }
        .video-js { width: 100%; height: 100%; max-width: 1280px; max-height: 720px; }
    </style>
</head>
<body>
    <div class="video-container">
        <video-js id="player" 
            class="vjs-default-skin vjs-big-play-centered" 
            controls 
            preload="auto">
            <source src="${masterPlaylistUrl}" type="application/x-mpegURL">
        </video-js>
    </div>
    <script>
        const player = videojs('player', {
            fluid: true,
            html5: {
                hls: {
                    enableLowInitialPlaylist: true,
                    smoothQualityChange: true,
                    overrideNative: true
                }
            }
        });
        player.play();
    </script>
</body>
</html>`;
    }

    private static isValidKey(key: string): boolean {
        return Boolean(key && key.match(/^[a-zA-Z0-9-_/]+$/));
    }

    private static getExpiryDate(expiresIn: number): string {
        return new Date(Date.now() + expiresIn * 1000).toISOString();
    }
}
