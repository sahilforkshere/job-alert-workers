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
    
    // 🌟 FIX 1: Safely extract the first URL from the new source_urls array
    const url = (j.source_urls && j.source_urls.length > 0) ? j.source_urls[0] : "#";
    
    // 🌟 FIX 2: Intelligently combine split location fields and work mode
    let locationParts = [];
    if (j.location_city && j.location_city !== 'NULL') locationParts.push(j.location_city);
    if (j.location_country && j.location_country !== 'NULL') locationParts.push(j.location_country);
    
    let location = locationParts.join(', ');
    
    // Append work mode (e.g., "Bangalore, India • HYBRID") or use it standalone if city is missing
    if (j.work_mode && j.work_mode !== 'NULL') {
      location = location ? `${location} &middot; ${j.work_mode}` : j.work_mode;
    }
    
    if (!location) {
      location = "Location Not Specified";
    }
    
    // 1. Dynamically extract the root domain from the job_url
    let domain = "";
    try {
      if (url.startsWith('http')) {
        const parsedUrl = new URL(url);
        // Extracts the hostname and removes 'www.' (e.g., 'careers.microsoft.com' becomes 'microsoft.com')
        domain = parsedUrl.hostname.replace(/^www\./, ''); 
      }
    } catch (err) {
      // Silently handle any invalid URLs
    }

    // 2. Fetch from Google Favicon (Zero Auth!). Fallback to UI-Avatars if the URL was missing/invalid.
    const logoUrl = domain 
      ? `https://www.google.com/s2/favicons?domain=${domain}&sz=128`
      : `https://ui-avatars.com/api/?name=${encodeURIComponent(company)}&background=f8fafc&color=475569&size=128`;

    // Email-safe Table Layout 
    return `
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom: 24px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <tr>
          <td width="64" valign="top" style="padding-right: 16px;">
            <img src="${logoUrl}" alt="${company} logo" width="64" height="64" style="display: block; width: 64px; height: 64px; object-fit: contain; background-color: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 4px;" />
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
          And ${overflowCount} more matches! 
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
      subject: `${jobs.length + (overflowCount || 0)} New Job Matches for You`,
      html: `
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
          <h2 style="color: #1a202c; font-size: 22px; margin-top: 0;">Latest Job Matches</h2>
          <p style="color: #4a5568; font-size: 15px; margin-bottom: 24px;">We found these new opportunities matching your preferences:</p>
          
          ${jobHtmlList}
          
          ${overflowHtml} 
          
          <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; text-align: center;">
            <p style="font-size: 12px; color: #a0aec0; line-height: 1.6; margin: 0 0 8px 0;">
              You are receiving this email because you opted into notifications.<br>
              <a href="{{COMPANY_SETTINGS_URL}}" style="color: #0066cc; text-decoration: underline;">Subscribe to Chromateo</a> to get regular, personalized job alerts tailored to your career goals.
            </p>
            <p style="font-size: 12px; margin: 0;">
              <a href="{{UNSUBSCRIBE_URL}}" style="color: #a0aec0; text-decoration: underline;">Unsubscribe from these alerts</a>
            </p>
          </div>
        </div>
      `
    });

    // GRANULAR POISON PILL CHECK (Inspect API Response)
    if (error) {
      const errCode = error.statusCode || error.code;
      
      if (errCode === 400 || errCode === 403 || errCode === 422) {
        console.error(`Terminal Error for ${email}: ${error.message}. Skipping retry (ACK Job).`);
        
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

    console.log(`Successfully sent digest to ${email} (including ${overflowCount || 0} overflow) and marked as SENT in DB`);
    return data;

  } catch (err) {
    console.error(`Transient failure during dispatch for ${email}, will retry:`, err.message);
    throw err; 
  }
}, { connection, limiter: { max: 1 , duration: 1000 } }); 

emailWorker.on('failed', (job, err) => {
  console.log(`Job ${job.id} (Email: ${job.data.email}) failed permanently after max retries: ${err.message}`);
});