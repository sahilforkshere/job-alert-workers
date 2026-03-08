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
  const { email, jobs, logIds, overflowCount } = job.data; 

  const jobHtmlList = jobs.map(j => {
    const title = j.job_title || "New Job Opportunity";
    const company = j.company_name || "Hiring Company";
    const industry = j.industry && j.industry !== 'NULL' ? j.industry : "Not Specified";
    const experience = j.experience && j.experience !== 'NULL' ? j.experience : "Any Experience";
    
    // --- NEW: Sanitize and Truncate Job Description ---
    // 1. Remove HTML tags using regex (RemoteOK fix)
    // 2. Normalize whitespace
    const cleanDesc = (j.job_description || '')
      .replace(/<[^>]*>?/gm, '') 
      .replace(/\s+/g, ' ')      
      .trim();

    // 3. Truncate to 180 characters for the digest view
    const truncatedDesc = cleanDesc.length > 180 
      ? cleanDesc.substring(0, 180) + '...' 
      : cleanDesc;

    // Extract URL
    const url = (j.source_urls && j.source_urls.length > 0) ? j.source_urls[0] : "#";
    
    // Combine Location
    let locationParts = [];
    if (j.location_city && j.location_city !== 'NULL') locationParts.push(j.location_city);
    if (j.location_country && j.location_country !== 'NULL') locationParts.push(j.location_country);
    let location = locationParts.join(', ') || "Remote / On-site";

    // Format Meta Badges (Mode, Type, Experience)
    const mode = j.work_mode && j.work_mode !== 'NULL' ? j.work_mode : "";
    const type = j.job_type && j.job_type !== 'NULL' ? j.job_type : "";

    return `
      <div style="margin-bottom: 32px; padding-bottom: 24px; border-bottom: 1px solid #edf2f7; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td>
              <div style="margin-bottom: 4px;">
                <a href="${url}" style="color: #2b6cb0; text-decoration: none; font-size: 18px; font-weight: 700; line-height: 1.2;">
                  ${title}
                </a>
              </div>
              
              <div style="color: #4a5568; font-size: 15px; font-weight: 500; margin-bottom: 8px;">
                ${company} <span style="color: #cbd5e0; margin: 0 6px;">&bull;</span> ${industry}
              </div>

              ${truncatedDesc ? `
              <div style="color: #4a5568; font-size: 14px; line-height: 1.5; margin-bottom: 12px;">
                ${truncatedDesc}
              </div>
              ` : ''}

              <div style="color: #718096; font-size: 14px; margin-bottom: 12px;">
                <strong>Location:</strong> ${location}
              </div>

              <div style="font-size: 12px; color: #4a5568;">
                ${mode ? `<span style="background-color: #ebf8ff; color: #2c5282; padding: 4px 8px; border-radius: 4px; margin-right: 8px; text-transform: uppercase; font-weight: bold;">${mode}</span>` : ''}
                ${type ? `<span style="background-color: #f0fff4; color: #22543d; padding: 4px 8px; border-radius: 4px; margin-right: 8px; text-transform: uppercase; font-weight: bold;">${type}</span>` : ''}
                <span style="background-color: #edf2f7; color: #2d3748; padding: 4px 8px; border-radius: 4px; text-transform: uppercase; font-weight: bold;">${experience}</span>
              </div>
            </td>
          </tr>
        </table>
      </div>
    `;
  }).join('');

  const overflowHtml = overflowCount > 0 
    ? `<div style="margin-top: 20px; padding: 16px; background-color: #f7fafc; border-radius: 8px; text-align: center; border: 1px dashed #cbd5e0;">
        <p style="color: #2d3748; margin: 0; font-weight: bold; font-size: 16px;">
          + ${overflowCount} more matching opportunities
        </p>
        <p style="color: #718096; font-size: 14px; margin: 4px 0 0 0;">
          View your full personalized feed on your dashboard.
        </p>
      </div>` 
    : '';

  try {
    const { error, data } = await resend.emails.send({
      from: 'Chromateo Job Alerts <notifications@mail.chromateo.com>',
      to: [email],
      subject: `${jobs.length + (overflowCount || 0)} New Openings Found: ${jobs[0].job_title}`,
      html: `
        <div style="max-width: 600px; margin: 0 auto; padding: 20px; background-color: #ffffff;">
          <h1 style="color: #1a202c; font-size: 24px; font-weight: 800; margin-bottom: 8px; letter-spacing: -0.02em;">New Job Matches</h1>
          <p style="color: #718096; font-size: 16px; margin-bottom: 32px;">Based on your profile, we've identified the following opportunities:</p>
          
          ${jobHtmlList}
          
          ${overflowHtml} 
          
          <div style="margin-top: 48px; padding-top: 24px; border-top: 2px solid #f7fafc; text-align: center;">
            <p style="font-size: 12px; color: #a0aec0; line-height: 1.6;">
              Sent via <strong>Chromateo</strong> &bull; Personalized Career Intelligence<br>
              <a href="{{COMPANY_SETTINGS_URL}}" style="color: #2b6cb0; text-decoration: none;">Profile Settings</a> &bull; 
              <a href="{{UNSUBSCRIBE_URL}}" style="color: #a0aec0; text-decoration: underline;">Unsubscribe</a>
            </p>
          </div>
        </div>
      `
    });

    if (error) {
      const errCode = error.statusCode || error.code;
      if (errCode === 400 || errCode === 403 || errCode === 422) {
        await supabase.from('alert_delivery_logs').update({ status: 'FAILED_PERMANENTLY' }).in('id', logIds);
        return { status: 'skipped', reason: 'terminal_api_error' }; 
      }
      throw new Error(`Resend Error: ${error.message}`);
    }
    
    const { error: updateError } = await supabase
      .from('alert_delivery_logs')
      .update({ status: 'SENT', sent_at: new Date().toISOString() })
      .in('id', logIds);

    if (updateError) throw updateError; 
    console.log(`✅ Digest sent to ${email}`);
    return data;

  } catch (err) {
    console.error(`❌ Dispatch failed for ${email}:`, err.message);
    throw err; 
  }
}, { connection, limiter: { max: 1 , duration: 1000 } }); 

emailWorker.on('failed', (job, err) => {
  console.log(`Job ${job.id} failed: ${err.message}`);
});