const { Worker } = require('bullmq');
const IORedis = require('ioredis');
const { Resend } = require('resend'); // Import Resend
require("dotenv").config();

// 1. Setup Connections
const resend = new Resend(process.env.RESEND_API_KEY);
const connection = new IORedis(process.env.UPSTASH_REDIS_TCP_URL, {
  maxRetriesPerRequest: null,
  tls: { rejectUnauthorized: false }
});

console.log("📬 BullMQ Email Delivery Worker is active and ready to send...");

// 2. Define the Worker logic
const emailWorker = new Worker('email_delivery_queue', async job => {
  const { email, job_title, company, job_url } = job.data;

  try {
    // 3. Dispatch the Real Email
    const { data, error } = await resend.emails.send({
      from: 'Job Alerts <onboarding@resend.dev>', // Use your verified domain in production
      to: [email],
      subject: `New Job Match: ${job_title} at ${company}`,
      html: `
        <h1>New Job Opportunity!</h1>
        <p>We found a match for you: <strong>${job_title}</strong> at <strong>${company}</strong>.</p>
        <p><a href="${job_url}" style="padding: 10px 20px; background-color: #007bff; color: white; text-decoration: none; border-radius: 5px;">View Job Details</a></p>
        <br>
        <p>Good luck with your application!</p>
      `
    });

    if (error) throw new Error(error.message);
    
    console.log(`📧 Email sent successfully to ${email} for Job ID: ${job.id}`);

  } catch (err) {
    console.error(`❌ Failed to send email to ${email}:`, err.message);
    throw err; // Trigger BullMQ retry logic
  }
}, { 
  connection,
  limiter: {
    max: 5,        // Conservative limit (5 per second) for Resend free tier
    duration: 1000 
  }
});

// Event Listeners for Monitoring
emailWorker.on('completed', job => console.log(`✅ Job ${job.id} finalized.`));
emailWorker.on('failed', (job, err) => console.error(`🚨 Job ${job.id} permanently failed:`, err.message));