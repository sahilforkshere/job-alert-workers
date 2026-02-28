const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const { Redis } = require("@upstash/redis");
const { createClient } = require("@supabase/supabase-js");
require("dotenv").config();

// 1. Connection Setup
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const restRedis = new Redis({ url: process.env.UPSTASH_REDIS_REST_URL, token: process.env.UPSTASH_REDIS_REST_TOKEN });

// BullMQ TCP Connection (Now works on Hotspot!)
const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL, {
  maxRetriesPerRequest: null,
  tls: { rejectUnauthorized: false }
});

const emailQueue = new Queue('email_delivery_queue', { connection });

async function startMatchingWorker() {
  console.log("🕵️ Matching Worker (BullMQ Production) is active...");

  while (true) {
    try {
      const rawData = await restRedis.rpop("matching_queue"); 
      if (!rawData) { await new Promise(r => setTimeout(r, 2000)); continue; }

      const jobData = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
      const { job_id, sector, location, experience_levels } = jobData;

      console.log(`🔍 Matching Job: ${job_id}`);

      // GIN-indexed matching logic [cite: 25]
      const { data: matches } = await supabase.rpc('find_matching_users', {
        input_sector: sector || 'Unknown',
        input_location: location || 'Unknown',
        input_experience: experience_levels || 'Unknown'
      });

      if (matches && matches.length > 0) {
        for (const user of matches) {
          // IDEMPOTENCY: Check if already alerted [cite: 28, 57]
          const { error: logError } = await supabase
            .from('alert_delivery_logs')
            .insert({ user_id: user.user_id, job_alert_id: job_id });

          if (logError && logError.code === '23505') continue; 

          // HANDOFF: Push to BullMQ Email Queue [cite: 30]
          await emailQueue.add('send-alert', {
            email: user.email,
            job_title: jobData.job_title || "New Job",
            company: jobData.company_name || "Unknown Company",
            job_url: jobData.job_url || "#"
          });
          console.log(`✅ Alert queued in BullMQ for ${user.email}`);
        }
      }
    } catch (err) {
      console.error("❌ Worker Error:", err.message);
    }
  }
}
startMatchingWorker();