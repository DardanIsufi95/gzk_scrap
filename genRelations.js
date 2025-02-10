import { Queue, Worker } from 'bullmq';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import axios from 'axios';
import jsdom from 'jsdom';
import e from 'express';
const { JSDOM } = jsdom;
const connection = { host: '127.0.0.1', port: 6379 };

const batchSize = 1000;
let offset = 0;

const db = await open({
    filename: 'gzk2.db',
    driver: sqlite3.Database
});

await db.exec(`CREATE TABLE IF NOT EXISTS relations (
    actid INTEGER,
    relType TEXT,
    relSubType TEXT,
    title TEXT
)`);
const sql = `SELECT actid, meta_html FROM pages LIMIT ? OFFSET ?`;

const distinct_relations = new Set();


async function processBatch() {
    const rows = await db.all(sql, [batchSize.toString(), offset.toString()]);

    for (const row of rows) {
        const dom = new JSDOM(row.meta_html);
        const document = dom.window.document;

        document.querySelectorAll('.act_link_documents').forEach((el) => {
            const relType = el.querySelector('h1').textContent.trim();
            console.log("relType", relType);
            el.querySelectorAll('[class^="act_link_box"]').forEach((el) => {
                
                const relSubType = el.querySelector('.act_link_box_1 > span.span_margin')?.textContent.trim() || '';
                const title = el.querySelector('[class^="act_detail_title"]')?.textContent.trim() || '';
            
                console.log({
                    actid: row.actid,
                    relType,
                    relSubType,
                    title
                })

                db.run(`INSERT INTO relations (actid, relType, relSubType, title) VALUES (?, ?, ?, ?)`, [row.actid, relType, relSubType, title]);
            });
            
            
        });
    }

    offset += batchSize;
    return rows.length;
}

async function main() {
    try {
        let run = 1 ;
        let rowsProcessed;
        do {
            rowsProcessed = await processBatch();

            run++;
        } while (rowsProcessed === batchSize);
    } catch (error) {
        console.error('Error processing batches:', error);
    } finally {
        db.close();
        console.log('Done');
    }
}

main();
