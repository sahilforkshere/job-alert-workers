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
  const { email, jobs, logIds } = job.data; 

  // THE FIX: Map the actual job data to HTML elements
  const jobHtmlList = jobs.map(j => {
    // We use fallbacks just in case the data is missing
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

  try {
    const { error } = await resend.emails.send({
      from: 'Job Alerts <notifications@mail.chromateo.com>',
      to: [email],
      subject: `🚀 ${jobs.length} New Job Matches for You`,
      html: `
        <div style="max-width: 600px; margin: 0 auto;">
          <h2 style="color: #2d3748;">Latest Job Matches</h2>
          <p style="color: #718096;">We found these new opportunities matching your preferences:</p>
          <hr style="border: 0; border-top: 1px solid #edf2f7; margin: 20px 0;" />
          ${jobHtmlList}
          <p style="font-size: 12px; color: #a0aec0; margin-top: 40px;">
            You are receiving this because of your job alert settings.
          </p>
        </div>
      `
    });

    if (error) throw new Error(error.message);
    
    // Update the database status to SENT so you can track it
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