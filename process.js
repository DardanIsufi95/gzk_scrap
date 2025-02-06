import { Queue ,Worker } from 'bullmq';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
import axios from 'axios';
import jsdom from 'jsdom';
import e from 'express';
const { JSDOM } = jsdom;
const connection = { host: '127.0.0.1', port: 6379 };



const db = await open({
    filename: 'database.db',
    driver: sqlite3.Database
})

await db.exec(`CREATE TABLE IF NOT EXISTS pages (
    actid INTEGER PRIMARY KEY,
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
    text_html TEXT
)`)

async function fetchActMeta(actid){
    return axios.get(`https://gzk.rks-gov.net/ActDetail.aspx?ActID=${actid}`).then((response) => {
        const dom = new JSDOM(response.data);
        const document = dom.window.document;
    
    
        return{
            title: document.querySelector('.act_detail_title_a a')?.textContent.trim(),
            haspdf: !!document.querySelector('.act_detail_download [src$="pdf32.png"]'),
            hastxt: !!document.querySelector('.act_detail_download [src$="txt32.png"]'),
            type: document.querySelector('.main_doc_info_left td:nth-child(3)')?.textContent.trim(),
            act_nr: document.querySelectorAll('.main_doc_info_left td:nth-child(3)')[1]?.textContent.trim(),
            institution: document.querySelectorAll('.main_doc_info_left td:nth-child(3)')[2]?.textContent.trim(),
            date: document.querySelectorAll('.main_doc_info_right td:nth-child(3)')[2]?.textContent.trim(),
            gz_nr: document.querySelectorAll('.main_doc_info_right td:nth-child(3)')[3]?.textContent.trim(),
            meta_html: document.querySelector('.content_main')?.innerHTML
        }
    })
}

async function fetchActText(actid){
    return axios.get(`https://gzk.rks-gov.net/ActDocumentDetail.aspx?ActID=${actid}`).then((response) => {
        const dom = new JSDOM(response.data);
        const document = dom.window.document;

        return {
            title: document.querySelector('.browse_acts .other_titles')?.textContent.trim(),
            text: document.querySelector('.content_main .main_doc_txt')?.textContent.trim(),
            html: document.querySelector('.content_main')?.innerHTML
        }
    })
}

const worker = new Worker(
    'pageQueue',
    async (job) => {
        

        const { actid} = job.data;


        const result = await Promise.all([ fetchActMeta(actid),fetchActText(actid)]).then(async ([meta, text]) => {
        
            if(!meta.title && !text.title) {
                return null;
            }
        
        
        
        
            await db.run(`INSERT INTO pages (actid,title, haspdf, hastxt, type, act_nr, institution, date, gz_nr, meta_html, text_title, text, text_html) VALUES (
                $actid , $title, $haspdf, $hastxt, $type, $act_nr, $institution, $date, $gz_nr, $meta_html, $text_title, $text, $text_html
            )`, {
                $actid: actid,
                $title: meta.title,
                $haspdf: meta.haspdf,
                $hastxt: meta.hastxt,
                $type: meta.type,
                $act_nr: meta.act_nr,
                $institution: meta.institution,
                $date: meta.date,
                $gz_nr: meta.gz_nr,
                $meta_html: meta.meta_html,
                $text_title: text.title,
                $text: text.text,
                $text_html: text.html
            })


            return {
                title: `${meta.title.slice(0, 70)}...`,
                act_nr: meta.act_nr,
                gz_nr: meta.gz_nr,
                id: actid

            }
        })


        if(!result) {
            return null;
        }

        return result;

    },
    {connection , concurrency: 10 },
);



worker.on('completed', (job) => {  
    if(job.returnvalue) {
        console.log(`Completed job ${job.returnvalue.actid} for ${job.returnvalue.title} (${job.returnvalue.gz_nr} - ${job.returnvalue.act_nr})`);
    }
})
