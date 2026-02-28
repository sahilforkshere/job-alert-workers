const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const { Resend } = require('resend');
const { createClient } = require('@supabase/supabase-js'); // NEW
require("dotenv").config();

const resend = new Resend(process.env.RESEND_API_KEY);
// NEW: Add Supabase to the email worker
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY); 

const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL, {
  maxRetriesPerRequest: null,
  tls: { rejectUnauthorized: false }
});

const emailWorker = new Worker('email_delivery_queue', async job => {
  // NEW: Pull logIds from the job data
  const { email, jobs, logIds } = job.data; 

  const jobHtmlList = jobs.map(j => `...`).join(''); // (Keep your existing HTML mapping here)

  try {
    const { error } = await resend.emails.send({
      from: 'Job Alerts <notifications@mail.chromateo.com>',
      to: [email],
      subject: `You have ${jobs.length} new job matches!`,
      html: `<h2>Latest job matches:</h2>${jobHtmlList}`
    });

    if (error) throw new Error(error.message);
    
    // NEW: Update the database status to SENT!
    await supabase
      .from('alert_delivery_logs')
      .update({ 
        status: 'SENT', 
        sent_at: new Date().toISOString() 
      })
      .in('id', logIds);

    console.log(`📧 Successfully sent digest to ${email} and marked as SENT in DB`);

  } catch (err) {
    console.error(`❌ Failed to send:`, err.message);
    throw err; 
  }
}, { connection, limiter: { max: 5, duration: 1000 } });