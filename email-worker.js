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
  // Destructure the overflowCount flag from the payload
  const { email, jobs, logIds, overflowCount } = job.data; 

  const jobHtmlList = jobs.map(j => {
    const title = j.job_title || "New Job Opportunity";
    const company = j.company_name || "Hiring Company";
    const url = j.job_url || "#";
    const location = j.location || "Remote / On-site"; // Fallback if location isn't provided
    
    // Using logo.dev's name search endpoint (Requires LOGO_DEV_PUBLISHABLE_KEY in .env)
    const logoUrl = `https://img.logo.dev/name/${encodeURIComponent(company)}?token=${process.env.LOGO_DEV_PUBLISHABLE_KEY}&size=128&format=png`;

    // Email-safe Table Layout to mimic the screenshot
    return `
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 24px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <tr>
          <td width="64" valign="top" style="padding-right: 16px;">
            <img src="${logoUrl}" alt="${company} logo" width="64" height="64" style="display: block; width: 64px; height: 64px; object-fit: contain; background-color: #ffffff; border-radius: 4px; border: 1px solid #e2e8f0;" />
          </td>
          
          <td valign="middle">
            <div style="margin: 0 0 4px 0;">
              <a href="${url}" style="color: #0066cc; text-decoration: none; font-size: 16px; font-weight: 500; line-height: 1.3;">
                ${title}
              </a>
            </div>
            <div style="color: #1a1a1a; font-size: 14px; line-height: 1.4;">
              ${company} &middot; ${location}
            </div>
          </td>
        </tr>
      </table>
    `;
  }).join('');

  // Conditionally render the Overflow Banner
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
      subject: `🚀 ${jobs.length + (overflowCount || 0)} New Job Matches for You`,
      html: `
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
          <h2 style="color: #1a202c; font-size: 22px; margin-top: 0;">Latest Job Matches</h2>
          <p style="color: #4a5568; font-size: 15px; margin-bottom: 24px;">We found these new opportunities matching your preferences:</p>
          
          ${jobHtmlList}
          
          ${overflowHtml} 
          
          <p style="font-size: 12px; color: #a0aec0; margin-top: 40px; text-align: center;">
            You are receiving this because of your job alert settings.
          </p>
        </div>
      `
    });

    // GRANULAR POISON PILL CHECK (Inspect API Response)
    if (error) {
      const errCode = error.statusCode || error.code;
      
      if (errCode === 400 || errCode === 403 || errCode === 422) {
        console.error(`🛑 Terminal Error for ${email}: ${error.message}. Skipping retry (ACK Job).`);
        
        await supabase
          .from('alert_delivery_logs')
          .update({ status: 'FAILED_PERMANENTLY' }) 
          .in('id', logIds);

        return { status: 'skipped', reason: 'invalid_email', api_error: error.message }; 
      }
      
      throw new Error(`Transient Resend API Error (${errCode}): ${error.message}`);
    }
    
    // Persistence: Atomic persistence state update.
    const { error: updateError } = await supabase
      .from('alert_delivery_logs')
      .update({ 
        status: 'SENT', 
        sent_at: new Date().toISOString() 
      })
      .in('id', logIds);

    if (updateError) throw updateError; 

    console.log(`📧 Successfully sent digest to ${email} (including ${overflowCount || 0} overflow) and marked as SENT in DB`);
    return data;

  } catch (err) {
    console.error(`❌ Transient failure during dispatch for ${email}, will retry:`, err.message);
    throw err; 
  }
}, { connection, limiter: { max: 1 , duration: 1000 } }); 

emailWorker.on('failed', (job, err) => {
  console.log(`❌ Job ${job.id} (Email: ${job.data.email}) failed permanently after max retries: ${err.message}`);
});