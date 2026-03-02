const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const { Resend } = require('resend');
const { createClient } = require('@supabase/supabase-js'); 
require("dotenv").config();

const resend = new Resend(process.env.RESEND_API_KEY);
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY); 

const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL, {
  maxRetriesPerRequest: null,
  tls: { rejectUnauthorized: false }
});

const emailWorker = new Worker('email_delivery_queue', async job => {
  // 🔥 FIX 2: Destructure the overflowCount flag from the payload
  const { email, jobs, logIds, overflowCount } = job.data; 

  const jobHtmlList = jobs.map(j => {
    const title = j.job_title || "New Job Opportunity";
    const company = j.company_name || "Hiring Company";
    const url = j.job_url || "#";

    return `
      <div style="margin-bottom: 24px; padding: 16px; border: 1px solid #e2e8f0; border-radius: 8px; font-family: sans-serif;">
        <h3 style="margin: 0 0 8px 0; color: #1a202c;">
          <a href="${url}" style="color: #3182ce; text-decoration: none;">${title}</a>
        </h3>
        <p style="margin: 0; color: #4a5568; font-weight: bold;">${company}</p>
        <div style="margin-top: 12px;">
          <a href="${url}" style="background-color: #3182ce; color: white; padding: 8px 16px; border-radius: 4px; text-decoration: none; font-size: 14px;">View Job</a>
        </div>
      </div>
    `;
  }).join('');

  // 🔥 FIX 2: Conditionally render the Overflow Banner
  const overflowHtml = overflowCount > 0 
    ? `<div style="margin-top: 20px; padding: 12px; background-color: #ebf8ff; border-radius: 6px; text-align: center; border: 1px solid #90cdf4;">
        <p style="color: #2b6cb0; margin: 0; font-weight: bold; font-size: 16px;">
          🔥 And ${overflowCount} more matches! 
        </p>
        <p style="color: #4a5568; font-size: 14px; margin: 4px 0 0 0;">
          Log in to your dashboard to view all your opportunities.
        </p>
      </div>` 
    : '';

  try {
    const { error, data } = await resend.emails.send({
      from: 'Job Alerts <notifications@mail.chromateo.com>',
      to: [email],
      // 🔥 FIX 2: Dynamic subject line to include total count
      subject: `🚀 ${jobs.length + (overflowCount || 0)} New Job Matches for You`,
      html: `
        <div style="max-width: 600px; margin: 0 auto; font-family: sans-serif;">
          <h2 style="color: #2d3748;">Latest Job Matches</h2>
          <p style="color: #718096;">We found these new opportunities matching your preferences:</p>
          <hr style="border: 0; border-top: 1px solid #edf2f7; margin: 20px 0;" />
          ${jobHtmlList}
          ${overflowHtml} <p style="font-size: 12px; color: #a0aec0; margin-top: 40px; text-align: center;">
            You are receiving this because of your job alert settings.
          </p>
        </div>
      `
    });

    // 🔥 FIX 1: GRANULAR POISON PILL CHECK (Inspect API Response)
    if (error) {
      const errCode = error.statusCode || error.code;
      
      // Handle Terminal Error (e.g., 400 Validation, 403 Bounce, 422 Invalid Email)
      if (errCode === 400 || errCode === 403 || errCode === 422) {
        console.error(`🛑 Terminal Error for ${email}: ${error.message}. Skipping retry (ACK Job).`);
        
        // Final DB Update for FAILED state (Fix: Status tracking detail)
        await supabase
          .from('alert_delivery_logs')
          .update({ status: 'FAILED_PERMANENTLY' }) // Use image_4.png failure state detail
          .in('id', logIds);

        // Return gracefully so BullMQ marks it 'completed' and drops it from the queue
        return { status: 'skipped', reason: 'invalid_email', api_error: error.message }; 
      }
      
      // For Transient Errors (429 Rate Limit, 500 Server Down), THROW to trigger retries
      throw new Error(`Transient Resend API Error (${errCode}): ${error.message}`);
    }
    
    // 🔥 FIX 4 (Persistence): Atomic persistence state update.
    const { error: updateError } = await supabase
      .from('alert_delivery_logs')
      .update({ 
        status: 'SENT', 
        sent_at: new Date().toISOString() 
      })
      .in('id', logIds);

    if (updateError) throw updateError; // Throw so BullMQ retries if Supabase fails (even if email sent)

    console.log(`📧 Successfully sent digest to ${email} (including ${overflowCount || 0} overflow) and marked as SENT in DB`);
    return data;

  } catch (err) {
    console.error(`❌ Transient failure during dispatch for ${email}, will retry:`, err.message);
    throw err; // NACK (Negative Acknowledgement): Trigger BullMQ retry mechanism
  }
}, { connection, limiter: { max: 5, duration: 1000 } }); // (Hardened rate limit detail)

emailWorker.on('failed', (job, err) => {
  console.log(`❌ Job ${job.id} (Email: ${job.data.email}) failed permanently after max retries: ${err.message}`);
  // You might want to log this to your database's `error_log` table here.
});