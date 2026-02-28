// test-push.js
const { Redis } = require("@upstash/redis");
require("dotenv").config();
const restRedis = new Redis({ url: process.env.UPSTASH_REDIS_REST_URL, token: process.env.UPSTASH_REDIS_REST_TOKEN });

async function pushTestJob() {
  const testJob = {
    job_id: "1c793eb7-731e-47be-bb78-9a32d39b50c4", // The Tesco Job UUID
    job_title: "Software Development Engineer III",
    company_name: "Tesco",
    sector: "Technology",
    location: "Bengaluru, Karnataka",
    experience_levels: "Senior",
    job_url: "https://careers.tesco.com/..."
  };
  await restRedis.lpush("matching_queue", JSON.stringify(testJob));
  console.log("🚀 Tesco job pushed to matching_queue!");
}
pushTestJob();