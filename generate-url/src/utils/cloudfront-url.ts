import { getSignedUrl } from '@aws-sdk/cloudfront-signer';
import { readFileSync } from 'fs';
import path from 'path';
import dotenv from 'dotenv';
import { GenerateUrlRequest } from '../types/video';

// Load environment variables
dotenv.config();

// Load the private key from environment or file
const PRIVATE_KEY = process.env.AWS_PRIVATE_KEY || readFileSync(path.resolve(__dirname,'..','..','private_key.pem'), 'utf-8');
const CLOUDFRONT_KEY_PAIR_ID = process.env.CLOUDFRONT_KEY_PAIR_ID;
const CLOUDFRONT_DOMAIN = process.env.CLOUDFRONT_DOMAIN;

if (!CLOUDFRONT_KEY_PAIR_ID || !CLOUDFRONT_DOMAIN) {
    throw new Error('Missing required environment variables: CLOUDFRONT_KEY_PAIR_ID, CLOUDFRONT_DOMAIN');
}


/**
 * Generates a signed URL for CloudFront distribution
 * @param options SignedUrlOptions
 * @returns signed URL string
 */
export function generateSignedUrl({ videoKey, expiresIn = 3600 }: GenerateUrlRequest): string {
    const url = new URL(videoKey, CLOUDFRONT_DOMAIN);
    
    const dateLessThan = new Date();
    dateLessThan.setSeconds(dateLessThan.getSeconds() + expiresIn);

    return getSignedUrl({
        url: url.toString(),
        keyPairId: CLOUDFRONT_KEY_PAIR_ID!,
        privateKey: PRIVATE_KEY,
        dateLessThan: dateLessThan.toISOString()
    });
}
