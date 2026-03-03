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
  const limit = 5000; 
  let totalProcessed = 0;

  // The 48-hour cutoff
  const expirationCutoff = new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString();

  // ==========================================
  // PHASE 1: Process and Queue Valid Jobs
  // ==========================================
  while (hasMore) {
    const { data: pending, error } = await supabase
      .from('alert_delivery_logs')
      .select(`
        id, 
        user_id, 
        profiles!inner(email, has_access), 
        job_alerts!inner(job_title, company_name, job_url, created_at) 
      `)
      .eq('status', 'PENDING')
      .eq('profiles.has_access', true)
      .gte('job_alerts.created_at', expirationCutoff) // Only grab fresh jobs
      .range(offset, offset + limit - 1);

    if (error) {
      console.error("❌ DB Fetch Error:", error);
      process.exit(1);
    }

    if (!pending || pending.length === 0) {
      hasMore = false;
      break;
    }

    const buckets = pending.reduce((acc, item) => {
      if (!acc[item.user_id]) acc[item.user_id] = { email: item.profiles.email, jobs: [], logIds: [] };
      acc[item.user_id].jobs.push(item.job_alerts);
      acc[item.user_id].logIds.push(item.id);
      return acc;
    }, {});

    for (const userId in buckets) {
      const { email, jobs, logIds } = buckets[userId];
      
      const MAX_JOBS = 30;
      const jobsToSend = jobs.slice(0, MAX_JOBS);
      const logIdsToUpdate = logIds.slice(0, MAX_JOBS);
      const overflowCount = jobs.length > MAX_JOBS ? jobs.length - MAX_JOBS : 0;

      try {
        const batchHash = crypto.createHash('sha256').update(logIdsToUpdate.sort().join(',')).digest('hex');
        const uniqueJobId = `digest_${userId}_${batchHash}`;

        await emailQueue.add('hourly-digest', { 
          email, 
          jobs: jobsToSend, 
          logIds: logIdsToUpdate,
          overflowCount
        }, { 
          jobId: uniqueJobId,
          removeOnComplete: true 
        });

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

    if (pending.length < limit) {
       hasMore = false; 
    } else {
       offset += limit; 
    }
  }
  
  console.log(`✅ Queued emails for ${totalProcessed} users.`);

  // ==========================================
  // PHASE 2: Mark Expired Jobs
  // ==========================================
  console.log("🧹 Checking for jobs older than 48 hours to mark as EXPIRED...");
  await cleanUpExpired(expirationCutoff);

  process.exit(0); 
}

// Helper function to handle the expiration cleanup safely in chunks
async function cleanUpExpired(expirationCutoff) {
  let hasMoreExpired = true;
  let totalExpired = 0;

  while (hasMoreExpired) {
    const { data: expiredLogs, error: fetchError } = await supabase
      .from('alert_delivery_logs')
      .select(`id, job_alerts!inner(created_at)`)
      .eq('status', 'PENDING')
      .lt('job_alerts.created_at', expirationCutoff) // Grab the old ones
      .limit(1000); // Chunk by 1000 to avoid DB strain

    if (fetchError) {
      console.error("❌ Error fetching expired logs:", fetchError.message);
      break;
    }

    if (!expiredLogs || expiredLogs.length === 0) {
      hasMoreExpired = false;
      break;
    }

    const idsToUpdate = expiredLogs.map(log => log.id);

    const { error: updateError } = await supabase
      .from('alert_delivery_logs')
      .update({ status: 'EXPIRED' })
      .in('id', idsToUpdate);

    if (updateError) {
      console.error("❌ Error updating expired logs:", updateError.message);
      break;
    }

    totalExpired += idsToUpdate.length;
  }

  console.log(`🏷️  Marked ${totalExpired} old PENDING logs as EXPIRED.`);
}

flushBuckets();