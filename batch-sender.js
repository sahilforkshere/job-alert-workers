const { Queue } = require('bullmq');
const IORedis = require('ioredis');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

// Setup Connections
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);
const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL, {
  maxRetriesPerRequest: null,
  tls: { rejectUnauthorized: false }
});
const emailQueue = new Queue('email_delivery_queue', { connection });

async function flushBuckets() {
  console.log("⏳ Checking for pending jobs to batch...");

  // 1. Fetch all PENDING logs
  const { data: pending } = await supabase
    .from('alert_delivery_logs')
    .select(`
      id, 
      user_id, 
      profiles!inner(email), 
      job_alerts!inner(job_title, company_name, job_url)
    `)
    .eq('status', 'PENDING');

  if (!pending || pending.length === 0) {
    console.log("📭 Buckets are empty. Nothing to send.");
    process.exit(0);
  }

  // 2. Group by User ID so they get ONE email
  const buckets = pending.reduce((acc, item) => {
    if (!acc[item.user_id]) acc[item.user_id] = { email: item.profiles.email, jobs: [], logIds: [] };
    acc[item.user_id].jobs.push(item.job_alerts);
    acc[item.user_id].logIds.push(item.id);
    return acc;
  }, {});

  // 3. Push the batched jobs to BullMQ
  for (const userId in buckets) {
    const { email, jobs, logIds } = buckets[userId];
    
// Change this line in your batch-sender.js
await emailQueue.add('hourly-digest', { email, jobs, logIds });
    console.log(`📤 Pushed digest for ${email} with ${jobs.length} jobs.`);

    // 4. Mark logs as QUEUED so they aren't processed again
    await supabase
      .from('alert_delivery_logs')
      .update({ status: 'QUEUED' })
      .in('id', logIds);
  }
  
  console.log(`✅ Successfully batched and queued emails for ${Object.keys(buckets).length} users.`);
  process.exit(0); // Exit the script so the cron job finishes
}

flushBuckets();