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

const RESOLUTIONS = [
  { name: "1080p", width: 1920, height: 1080 },
  { name: "720p", width: 1280, height: 720 },
  { name: "480p", width: 854, height: 480 },
  { name: "360p", width: 640, height: 360 },
];

// S3 path management
function generateS3Paths(originalKey) {
  // Extract course ID and filename from the original key
  // Expected format: courses/{courseId}/videos/{filename}
  const keyParts = originalKey.split('/');
  const courseId = keyParts[1];
  const filename = path.parse(keyParts[keyParts.length - 1]).name;
   
  return {
    courseId,
    filename,
    getTranscodedKey: (resolution) => 
      `courses/${courseId}/transcoded/${filename}/${resolution}.mp4`
  };
}

const s3client = new S3Client({
  region: "ap-south-1"
});

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
  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key,
  });
  const result = await s3client.send(command);
  const filePath = `original-${path.basename(key)}`;
  await fs.writeFile(filePath, result.Body);
  return path.resolve(filePath);
}

async function uploadToS3(filePath, bucket, key) {
  console.log(`Uploading ${filePath} to ${bucket}/${key}`);
  const fileContent = fsOld.createReadStream(filePath);
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileContent,
    ContentType: 'video/mp4',
  });
  return s3client.send(command);
}

function transcodeVideo(inputPath, outputPath, width, height) {
  return new Promise((resolve, reject) => {
    ffmpeg(inputPath)
      .output(outputPath)
      .videoCodec("libx264")
      .withAudioCodec("aac")
      .withSize(`${width}x${height}`)
      .format("mp4")
      .on("end", resolve)
      .on("error", reject)
      .run();
  });
}

async function cleanupFiles(files) {
  for (const file of files) {
    try {
      await fs.unlink(file);
    } catch (error) {
      console.warn(`Failed to delete file ${file}:`, error.message);
    }
  }
}

async function processVideo(sourceKey) {
  const filesToCleanup = [];
  const s3Paths = generateS3Paths(sourceKey);
  
  try {
    // Download original video
    const originalVideoPath = await downloadFromS3(
      process.env.AWS_S3_TEMP_BUCKET,
      sourceKey
    );
    filesToCleanup.push(originalVideoPath);

    // Process each resolution
    const transcodePromises = RESOLUTIONS.map(async ({ name, width, height }) => {
      const outputPath = `video-${s3Paths.filename}-${name}.mp4`;
      filesToCleanup.push(outputPath);
      const destinationKey = s3Paths.getTranscodedKey(name);

      try {
        // Check if file already exists
        const exists = await checkFileExists(process.env.AWS_S3_PROD_BUCKET, destinationKey);
        if (exists) {
          console.log(`File already exists: ${destinationKey}`);
          return;
        }

        // Transcode
        await transcodeVideo(originalVideoPath, outputPath, width, height);
        console.log(`Transcoding complete for ${name}`);

        // Upload
        await uploadToS3(
          outputPath,
          process.env.AWS_S3_PROD_BUCKET,
          destinationKey
        );
        console.log(`Uploaded ${outputPath} to S3`);
      } catch (error) {
        console.error(`Failed to process ${name} resolution:`, error);
        throw error;
      }
    });

    await Promise.all(transcodePromises);
    console.log(`All transcoding jobs completed successfully for ${sourceKey}`);
    return true;
  } catch (error) {
    console.error(`Transcoding process failed for ${sourceKey}:`, error);
    throw error;
  } finally {
    // Cleanup temporary files
    await cleanupFiles(filesToCleanup);
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