const { Redis } = require("@upstash/redis");
const { createClient } = require("@supabase/supabase-js");
require("dotenv").config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const restRedis = new Redis({ url: process.env.UPSTASH_REDIS_REST_URL, token: process.env.UPSTASH_REDIS_REST_TOKEN });

async function startMatchingWorker() {
  console.log("🕵️ Matching Worker (Batch Mode) is active...");

  while (true) {
    try {
      const rawData = await restRedis.rpop("matching_queue"); 
      if (!rawData) { await new Promise(r => setTimeout(r, 2000)); continue; }

      let targetJobId;
      try {
        const parsed = typeof rawData === 'string' ? JSON.parse(rawData) : rawData;
        targetJobId = parsed.job_id || parsed.id;
      } catch (e) {
        targetJobId = rawData; 
      }

      console.log(`\n🔍 Popped Job ID: ${targetJobId}`);

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

      // Helper to guarantee array format for PostgreSQL TEXT[] arguments
      const toArray = (val) => Array.isArray(val) ? val : (val ? [val] : ['Unknown']);

      const { data: matches, error: rpcError } = await supabase.rpc('find_matching_users', {
        input_job_title: currentJob.job_title || '',
        input_sector: toArray(currentJob.sector),
        input_location: toArray(currentJob.location),
        input_experience: toArray(currentJob.experience_levels)
      });

      if (rpcError) {
          console.error(`❌ RPC Error:`, rpcError.message);
          continue; // Skip inserting if the RPC failed
      }

      if (matches && matches.length > 0) {
        // Deduplicate multiple preference collisions in memory
        const uniqueUserIds = [...new Set(matches.map(user => user.user_id))];

        // Prepare the bulk payload
        const insertPayload = uniqueUserIds.map(userId => ({
          user_id: userId,
          job_alert_id: targetJobId,
          status: 'PENDING'
        }));

        // Single Bulk Upsert instead of a loop.
        const { error: logError } = await supabase
          .from('alert_delivery_logs')
          .upsert(insertPayload, { onConflict: 'user_id, job_alert_id', ignoreDuplicates: true });

        if (logError) {
           console.error(`❌ Bulk Insert Failed:`, logError.message);
        } else {
           console.log(`📥 Bulk stored ${insertPayload.length} matches in bucket (PENDING)`);
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