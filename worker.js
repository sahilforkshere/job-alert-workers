const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');
require('dotenv').config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL, {
  maxRetriesPerRequest: null,
  tls: { rejectUnauthorized: false }
});
const emailQueue = new Queue('email_delivery_queue', { connection });

async function flushBuckets() {
  console.log("⏳ Checking for pending jobs to batch...");

  let hasMore = true;
  let offset = 0;
  const limit = 5000; // 🔥 EDGE CASE 2 FIX: Process in safe memory chunks
  let totalProcessed = 0;

  // 🔥 THE FIX 1: Calculate the exact cutoff time (48 hours ago)
  const expirationCutoff = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

  while (hasMore) {
    // 1. Fetch PENDING logs with Pagination and Access Check
    const { data: pending, error } = await supabase
      .from('alert_delivery_logs') // [cite: 94]
      .select(`
        id, 
        user_id, 
        profiles!inner(email, has_access), 
        job_alerts!inner(job_title, company_name, job_url, created_at) 
      `) // 🔥 Added created_at here
      .eq('status', 'PENDING')
      .eq('profiles.has_access', true) // 🔥 EDGE CASE 4 FIX: Ghost Subscriber check 
      .gte('job_alerts.created_at', expirationCutoff) // 🔥 THE FIX 1: Ignore old jobs
      .range(offset, offset + limit - 1);

    if (error) {
      console.error("❌ DB Fetch Error:", error);
      process.exit(1);
    }

    if (!pending || pending.length === 0) {
      hasMore = false;
      break;
    }

    // 2. Group by User ID
    const buckets = pending.reduce((acc, item) => {
      if (!acc[item.user_id]) acc[item.user_id] = { email: item.profiles.email, jobs: [], logIds: [] };
      acc[item.user_id].jobs.push(item.job_alerts);
      acc[item.user_id].logIds.push(item.id);
      return acc;
    }, {});

    // 3. Safely Push to BullMQ
    for (const userId in buckets) {
      const { email, jobs, logIds } = buckets[userId];
      
      // 🔥 THE FIX 2: Payload Capping (Roll-over Queue)
      const MAX_JOBS = 30;
      const jobsToSend = jobs.slice(0, MAX_JOBS);
      const logIdsToUpdate = logIds.slice(0, MAX_JOBS);
      const overflowCount = jobs.length > MAX_JOBS ? jobs.length - MAX_JOBS : 0;

      try {
        const batchHash = crypto.createHash('sha256').update(logIdsToUpdate.sort().join(',')).digest('hex');
        const uniqueJobId = `digest_${userId}_${batchHash}`;

        // 🔥 Updated payload to include sliced jobs, sliced IDs, and overflowCount
        await emailQueue.add('hourly-digest', { 
          email, 
          jobs: jobsToSend, 
          logIds: logIdsToUpdate,
          overflowCount
        }, { 
          jobId: uniqueJobId,
          removeOnComplete: true 
        });

        // 🔥 Update ONLY the sliced logIds to QUEUED
        const { error: updateError } = await supabase
          .from('alert_delivery_logs')
          .update({ status: 'QUEUED' })
          .in('id', logIdsToUpdate);

        if (updateError) throw updateError;
        totalProcessed++;
        
      } catch (err) {
        console.error(`⚠️ Failed to process batch for user ${userId}:`, err.message);
      }
    }

    // Move to the next page of results
    if (pending.length < limit) {
       hasMore = false; // Last page reached
    } else {
       offset += limit; 
    }
  }
  
  console.log(`✅ Successfully batched and queued emails for ${totalProcessed} users.`);
  process.exit(0); 
}

flushBuckets();