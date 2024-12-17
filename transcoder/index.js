require("dotenv").config();
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
} = require("@aws-sdk/client-s3");
const fsOld = require("node:fs");
const fs = require("node:fs/promises");
const path = require("node:path");
const ffmpeg = require("fluent-ffmpeg");

// Validation of required environment variables
const requiredEnvVars = [
  "AWS_S3_TEMP_BUCKET",
  "AWS_S3_TEMP_KEY",
  "AWS_S3_PROD_BUCKET",
  "AWS_REGION",
];

// Configuration constants
const CONFIG = {
  SEGMENT_DURATION: 4,
  GOP_SIZE: 48,
  MAX_RETRIES: 3,
  RETRY_DELAY: 1000,
  UPLOAD_CONCURRENCY: 5,
  TEMP_DIR_PREFIX: 'hls-',
};

const RESOLUTIONS = [
  { name: "1080p", width: 1920, height: 1080, bitrate: "5000k" },
  { name: "720p", width: 1280, height: 720, bitrate: "2800k" },
  { name: "480p", width: 854, height: 480, bitrate: "1400k" },
  { name: "360p", width: 640, height: 360, bitrate: "800k" },
];

function validateEnvironment() {
  const missingVars = requiredEnvVars.filter(
    (varName) => !process.env[varName]
  );
  if (missingVars.length > 0) {
    throw new Error(
      `Missing required environment variables: ${missingVars.join(", ")}`
    );
  }
}

// S3 path management
function generateS3Paths(originalKey) {
  const keyParts = originalKey.split('/');
  const courseId = keyParts[1];
  const filename = path.parse(keyParts[keyParts.length - 1]).name;
   
  return {
    courseId,
    filename,
    getHLSBasePath: () => `${courseId}/hls/${filename}`,
    getMasterPlaylistKey: () => `${courseId}/hls/${filename}/master.m3u8`,
    getVariantBasePath: (resolution) => `${courseId}/hls/${filename}/${resolution}`,
    getVariantPlaylistKey: (resolution) => `${courseId}/hls/${filename}/${resolution}/playlist.m3u8`
  };
}

const s3client = new S3Client({
  region: process.env.AWS_REGION,
  maxAttempts: CONFIG.MAX_RETRIES,
});

// Utility function for exponential backoff
async function withRetry(operation, maxRetries = CONFIG.MAX_RETRIES) {
  let lastError;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      if (attempt === maxRetries) break;
      
      const delay = Math.min(CONFIG.RETRY_DELAY * Math.pow(2, attempt - 1), 10000);
      console.log(`Retry attempt ${attempt} after ${delay}ms`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  throw lastError;
}

async function checkFileExists(bucket, key) {
  try {
    await s3client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch (error) {
    if (error.name === 'NotFound') return false;
    throw error;
  }
}

async function downloadFromS3(bucket, key) {
  console.log(`Downloading ${key} from ${bucket}`);
  return withRetry(async () => {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });
    const result = await s3client.send(command);
    const filePath = `original-${path.basename(key)}`;
    
    // Get the content length from the response
    const contentLength = result.ContentLength;
    console.log(`File size from S3: ${contentLength} bytes`);
    
    // Stream to file instead of loading into memory
    const writeStream = fsOld.createWriteStream(filePath);
    await new Promise((resolve, reject) => {
      result.Body.pipe(writeStream)
        .on('error', reject)
        .on('finish', resolve);
    });
    
    // Verify the downloaded file
    const stats = await fs.stat(filePath);
    console.log(`Downloaded file size: ${stats.size} bytes`);
    
    if (stats.size === 0) {
      throw new Error('Downloaded file is empty');
    }
    
    return path.resolve(filePath);
  });
}

function transcodeToHLS(inputPath, outputDir, resolution) {
  return new Promise((resolve, reject) => {
    let startTime = Date.now();
    
    // First check if input file exists and is readable
    if (!fsOld.existsSync(inputPath)) {
      reject(new Error(`Input file does not exist: ${inputPath}`));
      return;
    }
    
    // Get input file information before transcoding
    ffmpeg.ffprobe(inputPath, (err, metadata) => {
      if (err) {
        reject(new Error(`Failed to probe input file: ${err.message}`));
        return;
      }
      
      console.log(`Input file metadata:`, {
        format: metadata.format.format_name,
        duration: metadata.format.duration,
        size: metadata.format.size,
        bitrate: metadata.format.bit_rate
      });
      
      ffmpeg(inputPath)
        .output(`${outputDir}/${resolution.name}/playlist.m3u8`)
        .videoCodec('libx264')
        .audioCodec('aac')
        .addOptions([
          '-profile:v high', // Changed from main to high profile for 10-bit support
          '-pix_fmt yuv420p', // Force 8-bit pixel format
          '-preset:v medium',
          '-crf 23',
          '-sc_threshold 0',
          '-g', CONFIG.GOP_SIZE.toString(),
          '-keyint_min', CONFIG.GOP_SIZE.toString(),
          '-hls_time', CONFIG.SEGMENT_DURATION.toString(),
          '-hls_playlist_type vod',
          '-b_strategy 0',
          '-use_timeline 1',
          '-use_template 1',
          '-movflags +faststart',
          '-max_muxing_queue_size 1024', // Added for complex transcoding
          '-hls_segment_filename', `${outputDir}/${resolution.name}/segment_%03d.ts`
        ])
        .size(`${resolution.width}x${resolution.height}`)
        .videoBitrate(resolution.bitrate)
        .audioChannels(2)
        .audioFrequency(44100)
        .audioBitrate('128k')
        .on('start', (commandLine) => {
          console.log(`[${resolution.name}] FFmpeg command: ${commandLine}`);
        })
        .on('progress', (progress) => {
          console.log(`[${resolution.name}] Processing: ${progress.percent}% done, ${progress.currentFps} fps`);
        })
        .on('end', () => {
          const duration = (Date.now() - startTime) / 1000;
          console.log(`[${resolution.name}] Transcoding completed in ${duration.toFixed(2)}s`);
          resolve();
        })
        .on('error', (err, stdout, stderr) => {
          console.error(`[${resolution.name}] FFmpeg stderr:`, stderr);
          reject(new Error(`Transcoding failed: ${err.message}\nStderr: ${stderr}`));
        })
        .run();
    });
  });
}

async function uploadToS3(filePath, bucket, key, contentType = 'application/x-mpegURL') {
  console.log(`Uploading ${filePath} to ${bucket}/${key}`);
  return withRetry(async () => {
    const fileContent = fsOld.createReadStream(filePath);
    const command = new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: fileContent,
      ContentType: contentType,
      CacheControl: 'max-age=31536000',
      Metadata: {
        'transcoded-date': new Date().toISOString(),
        'transcoder-version': '1.0.0'
      }
    });
    return s3client.send(command);
  });
}

function generateMasterPlaylist(variants) {
  let playlist = '#EXTM3U\n';
  playlist += '#EXT-X-VERSION:3\n';
  playlist += '#EXT-X-INDEPENDENT-SEGMENTS\n\n';
  
  variants.forEach(({ resolution, bandwidth }) => {
    playlist += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${resolution},CODECS="avc1.4d401f,mp4a.40.2"\n`;
    playlist += `${resolution}/playlist.m3u8\n`;
  });
  
  return playlist;
}

async function uploadHLSFiles(outputDir, resolution, bucket, s3BasePath) {
  const files = await fs.readdir(`${outputDir}/${resolution.name}`);
  
  // Upload files in parallel with limited concurrency
  const chunks = [];
  for (let i = 0; i < files.length; i += CONFIG.UPLOAD_CONCURRENCY) {
    chunks.push(files.slice(i, i + CONFIG.UPLOAD_CONCURRENCY));
  }

  for (const chunk of chunks) {
    await Promise.all(chunk.map(async (file) => {
      const filePath = `${outputDir}/${resolution.name}/${file}`;
      const s3Key = `${s3BasePath}/${resolution.name}/${file}`;
      const contentType = file.endsWith('.m3u8') ? 'application/x-mpegURL' : 'video/MP2T';
      
      await uploadToS3(filePath, bucket, s3Key, contentType);
    }));
  }
}

async function processVideo(sourceKey) {
  console.log(`Starting video processing for ${sourceKey}`);
  const startTime = Date.now();
  const s3Paths = generateS3Paths(sourceKey);
  const workDir = await fs.mkdtemp(CONFIG.TEMP_DIR_PREFIX);
  let originalVideoPath = null;
  
  try {
    // Download original video
    originalVideoPath = await downloadFromS3(
      process.env.AWS_S3_TEMP_BUCKET,
      sourceKey
    );
    
    console.log(`Successfully downloaded file to ${originalVideoPath}`);

    // Create output directory structure
    for (const resolution of RESOLUTIONS) {
      await fs.mkdir(`${workDir}/${resolution.name}`, { recursive: true });
    }

    // Process one resolution first as a test
    const testResolution = RESOLUTIONS[RESOLUTIONS.length - 1]; // Start with lowest resolution
    console.log(`Testing transcoding with ${testResolution.name} resolution first`);
    
    try {
      await transcodeToHLS(originalVideoPath, workDir, testResolution);
      console.log(`Test transcoding successful for ${testResolution.name}`);
    } catch (error) {
      throw new Error(`Test transcoding failed: ${error.message}`);
    }

    // If test succeeds, process remaining resolutions
    const remainingResolutions = RESOLUTIONS.slice(0, -1);
    const transcodePromises = remainingResolutions.map(async (resolution) => {
      const variantPath = s3Paths.getVariantBasePath(resolution.name);
      
      try {
        // Check if variant playlist already exists
        const exists = await checkFileExists(
          process.env.AWS_S3_PROD_BUCKET,
          s3Paths.getVariantPlaylistKey(resolution.name)
        );
        
        if (exists) {
          console.log(`Variant ${resolution.name} already exists, skipping`);
          return;
        }

        // Transcode to HLS
        await transcodeToHLS(originalVideoPath, workDir, resolution);

        // Upload segments and playlist
        await uploadHLSFiles(workDir, resolution, process.env.AWS_S3_PROD_BUCKET, s3Paths.getHLSBasePath());
        console.log(`Uploaded HLS files for ${resolution.name}`);
      } catch (error) {
        console.error(`Failed to process ${resolution.name}:`, error);
        throw error;
      }
    });

    await Promise.all(transcodePromises);

    // Generate and upload master playlist
    const masterPlaylist = generateMasterPlaylist(
      RESOLUTIONS.map(res => ({
        resolution: `${res.width}x${res.height}`,
        bandwidth: parseInt(res.bitrate) * 1000
      }))
    );

    const masterPlaylistPath = `${workDir}/master.m3u8`;
    await fs.writeFile(masterPlaylistPath, masterPlaylist);
    await uploadToS3(
      masterPlaylistPath,
      process.env.AWS_S3_PROD_BUCKET,
      s3Paths.getMasterPlaylistKey()
    );

    const duration = (Date.now() - startTime) / 1000;
    console.log(`HLS transcoding completed successfully for ${sourceKey} in ${duration.toFixed(2)}s`);
    return true;
  } catch (error) {
    console.error(`HLS transcoding failed for ${sourceKey}:`, error);
    throw error;
  } finally {
    // Cleanup
    try {
      await fs.rm(workDir, { recursive: true, force: true });
    } catch (error) {
      console.warn(`Failed to cleanup directory ${workDir}:`, error.message);
    }
  }
}

async function init() {
  try {
    validateEnvironment();
    await processVideo(process.env.AWS_S3_TEMP_KEY);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
}

init();