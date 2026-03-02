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
      // 1. Pop the payload from Redis
      const rawData = await restRedis.rpop("matching_queue"); 
      if (!rawData) { await new Promise(r => setTimeout(r, 2000)); continue; }

      // 2. Safely extract just the job_id
      let targetJobId;
      try {
        const parsed = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
        targetJobId = parsed.job_id || parsed.id;
      } catch (e) {
        targetJobId = rawData; // Fallback if Redis pushed a raw string UUID
      }

      console.log(`\n🔍 Popped Job ID: ${targetJobId}`);

      // 3. THE FIX: Fetch the actual job details from the database!
      const { data: currentJob, error: fetchError } = await supabase
        .from('job_alerts')
        .select('*')
        .eq('id', targetJobId)
        .single();

      if (fetchError || !currentJob) {
        console.error(`❌ Could not find job ${targetJobId} in DB. Skipping.`);
        continue; 
      }

      console.log(`✅ DB Fetch Success: ${currentJob.job_title} at ${currentJob.company_name}`);

      // 4. Pass the REAL data into your Full-Text Search RPC
      const { data: matches, error: rpcError } = await supabase.rpc('find_matching_users', {
        input_job_title: currentJob.job_title || '',
        input_sector: currentJob.sector || 'Unknown',
        input_location: currentJob.location || 'Unknown',
        input_experience: currentJob.experience_levels || 'Unknown'
      });

      if (matches && matches.length > 0) {
        for (const user of matches) {
          // 5. Drop into the bucket (Idempotency Check)
          const { error: logError } = await supabase
            .from('alert_delivery_logs')
            .insert({ 
              user_id: user.user_id, 
              job_alert_id: targetJobId,
              status: 'PENDING' 
            });

          if (logError && logError.code === '23505') {
               console.log(`⏩ Match already in bucket for ${user.email}, skipping.`);
          } else if (!logError) {
            console.log(`📥 Match stored in bucket (PENDING) for ${user.email}`);
          }
        }
      } else {
         console.log(`🤷‍♂️ No users matched for ${currentJob.job_title}`);
      }
    } catch (err) {
      console.error("❌ Worker Error:", err.message);
    }
  }
}

startMatchingWorker();