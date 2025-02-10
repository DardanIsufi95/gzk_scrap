import puppeteer from 'puppeteer';
import fs from 'fs';
import crypto from 'crypto';
import { Queue, Worker } from 'bullmq';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { JSDOM } from 'jsdom';
import path from 'path';

// If Node <18, you may need: import fetch from 'node-fetch';

const connection = { host: '127.0.0.1', port: 6379 };

// Optional: add global error handlers to avoid crashing on unhandled rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception:', err);
  // Usually you'd do a graceful shutdown here.
});

// Initialize SQLite
const db = await open({
  filename: 'gzk2.db',
  driver: sqlite3.Database,
});

await db.exec(`
  CREATE TABLE IF NOT EXISTS pages (
    actid INTEGER PRIMARY KEY,
    isActive INTEGER DEFAULT 1,
    title TEXT,
    haspdf INTEGER,
    hastxt INTEGER,
    type TEXT,
    act_nr TEXT,
    institution TEXT,
    date TEXT,
    gz_nr TEXT,
    meta_html TEXT,
    text_title TEXT,
    text TEXT,
    text_html TEXT,
    pdf_file_path TEXT
  )
`);

/**
 * Download the PDF for a given actid from gzk.rks-gov.net
 * using Puppeteer + waitForResponse approach.
 */
async function downloadPDF(actid) {
  let result = null;
  const browser = await puppeteer.launch({
    headless: true, // set to false if you need to see the browser
    args: ['--start-maximized'],
  });

  try {
    const page = await browser.newPage();
    await page.setDefaultTimeout(60000); // 60-second default timeout

    // Go to the page
    const url = `https://gzk.rks-gov.net/ActDocumentDetail.aspx?ActID=${actid}`;
    console.log(`[${actid}] Navigating to: ${url}`);
    await page.goto(url, { waitUntil: 'domcontentloaded' });

    // Wait for the PDF download button to appear
    await page.waitForSelector('.main_doc_download input[src$="pdf32.png"]', { timeout: 30000 });
    console.log(`[${actid}] Found PDF download button, clicking...`);

    // Click the download button, and in parallel, wait for the response that contains the PDF
    // Adjust your "waitForResponse" condition to match the actual PDF request
    const [response] = await Promise.all([
      page.waitForResponse(async (res) => {
        // Check if the request matches the PDF endpoint
        // This may be a GET or a POST; adjust if needed
        const req = res.request();
        const reqUrl = req.url();

        // If site uses POST to the same ActDocumentDetail.aspx with a parameter, for example:
        const isPost = req.method() === 'POST' && reqUrl.includes('ActDocumentDetail.aspx?ActID=');
        // If it uses GET, or a different endpoint, adjust accordingly:
        // const isGet = req.method() === 'GET' && /something/.test(reqUrl);

        if (isPost) {
          // Wait until the server responds with a PDF content-type
          const contentType = res.headers()['content-type'] || '';
          return contentType.includes('application/pdf');
        }
        return false;
      }, { timeout: 30000 }), // up to 30s for the response
      page.click('.main_doc_download input[src$="pdf32.png"]'),
    ]);

    // If we got here, we should have the PDF response
    if (!response) {
      throw new Error(`[${actid}] PDF response not found (timed out).`);
    }

    const contentType = response.headers()['content-type'] || '';
    if (!contentType.includes('application/pdf')) {
      throw new Error(`[${actid}] Response did not contain a PDF (content-type: ${contentType}).`);
    }

    console.log(`[${actid}] PDF response received, reading data...`);
    const buffer = await response.buffer();

    // Attempt to parse filename from content-disposition header
    let filename = `document_${crypto.randomUUID()}.pdf`;
    const contentDisposition = response.headers()['content-disposition'] || '';
    const filenameMatch = contentDisposition.match(/filename=(.+)/i);
    if (filenameMatch && filenameMatch[1]) {
      filename = filenameMatch[1].replace(/["']/g, '').trim();
    }

    const finalName = `${actid}_${filename}`;
    fs.writeFileSync(path.join('pdfs', finalName), buffer);

    console.log(`[${actid}] PDF saved as: ${finalName}`);
    result = { filename: finalName, actid };

  } catch (err) {
    console.error(`[${actid}] Error while downloading PDF:`, err);
  } finally {
    await browser.close();
  }
  return result;
}

// BullMQ Worker
const worker = new Worker(
  'pageQueue',
  async (job) => {
    const { actid } = job.data;
    console.log(`Processing job for actid: ${actid}`);
    const pdf = await downloadPDF(actid);

    // If the PDF download fails or doesn't have a filename, fail the job
    if (!pdf || !pdf.filename) {
      throw new Error(`PDF not downloaded successfully for actid: ${actid}`);
    }

    // Otherwise, update DB record
    await db.run(`
      UPDATE pages
      SET pdf_file_path = ?
      WHERE actid = ?
    `, [pdf.filename, actid]);

    return {
      actid,
      pdf_file_path: pdf.filename,
    };
  },
  {
    connection,
    concurrency: 5,
  }
);

// On job success
worker.on('completed', (job, returnValue) => {
  if (returnValue) {
    console.log(
      `Job ${job.id} completed. ActID: ${returnValue.actid}, PDF: ${returnValue.pdf_file_path}`
    );
  } else {
    console.log(`Job ${job.id} completed with no return value.`);
  }
});

// On job failure
worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed:`, err);
});

// On worker/connection error
worker.on('error', (err) => {
  console.error('Worker encountered an error:', err);
});
