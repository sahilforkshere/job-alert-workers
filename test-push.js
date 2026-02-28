const { Redis } = require("@upstash/redis");
require("dotenv").config();

// Make sure your token variable name matches your .env exactly
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN, 
});

async function pushPerfectJob() {
  const dummyJob = {
    job_id: "test-job-999",
    sector: "Technology", // Assuming you have a user with 'Technology' preference
    location: "Remote",   // Assuming you have a user with 'Remote' preference
    experience_levels: "Entry Level (0-1 years)" 
  };

  await redis.lpush("matching_queue", JSON.stringify(dummyJob));
  console.log("✅ Perfect dummy job pushed to Redis!");
}

pushPerfectJob();