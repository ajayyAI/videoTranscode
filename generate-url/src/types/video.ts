export interface GenerateUrlRequest {
    videoKey: string;
    expiresIn?: number;
}

export interface StreamRequest {
    courseId: string;
    videoId: string;
    quality?: VideoQuality;
}

export type VideoQuality = '360p' | '480p' | '720p' | '1080p';

export interface VideoResponse {
    success: boolean;
    data?: {
        url?: string;
        masterPlaylist?: string;
        qualityPlaylist?: string;
        quality?: VideoQuality;
        expiresIn: number;
        expiresAt: string;
    };
    error?: string;
}
