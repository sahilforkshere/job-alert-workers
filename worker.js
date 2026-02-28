const { Redis } = require("@upstash/redis");
const { createClient } = require("@supabase/supabase-js");
require("dotenv").config();

// 1. Connection Setup
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const restRedis = new Redis({ url: process.env.UPSTASH_REDIS_REST_URL, token: process.env.UPSTASH_REDIS_REST_TOKEN });

// 🚫 REMOVED BullMQ & IORedis from here. This script no longer sends emails directly!

async function startMatchingWorker() {
  console.log("🕵️ Matching Worker (Batch Mode) is active...");

  while (true) {
    try {
      const rawData = await restRedis.rpop("matching_queue"); 
      if (!rawData) { await new Promise(r => setTimeout(r, 2000)); continue; }

      const jobData = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
      // Extract the job_title from your Redis payload
const { job_id, job_title, sector, location, experience_levels } = jobData;

console.log(`🔍 Matching Job: ${job_id} - ${job_title}`);

// Pass the job_title into the new RPC function
const { data: matches, error: rpcError } = await supabase.rpc('find_matching_users', {
  input_job_title: job_title || '',       // <-- THIS IS THE NEW LINE
  input_sector: sector || 'Unknown',
  input_location: location || 'Unknown',
  input_experience: experience_levels || 'Unknown'
});

      if (matches && matches.length > 0) {
        for (const user of matches) {
          
          // IDEMPOTENCY: Check if already alerted and drop into the bucket [cite: 28, 57]
          const { error: logError } = await supabase
            .from('alert_delivery_logs')
            .insert({ 
              user_id: user.user_id, 
              job_alert_id: job_id,
              status: 'PENDING' // 👈 NEW: This marks it for the hourly batch
            });

          if (logError) {
            // Postgres error 23505 means the Unique Constraint blocked a duplicate
            if (logError.code === '23505') {
               console.log(`⏩ Match already in bucket for ${user.email}, skipping.`);
            } else {
               console.error("❌ SUPABASE LOG ERROR:", logError.message);
            }
          } else {
            console.log(`📥 Match safely stored in bucket (PENDING) for ${user.email}`);
          }
          
          // 🚫 REMOVED: emailQueue.add() - The hourly cron script will handle this now.
        }
      }
    } catch (err) {
      console.error("❌ Worker Error:", err.message);
    }
  }
}

startMatchingWorker();