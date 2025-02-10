import { Queue  } from 'bullmq';
import sqlite3 from 'sqlite3'
import { open } from 'sqlite'
const connection = { host: '127.0.0.1', port: 6379 };


const pageQueue = new Queue('pageQueue', { connection });
const db = await open({
    filename: 'gzk.db',
    driver: sqlite3.Database
})


const rows = await db.all(`SELECT actid FROM pages WHERE type NOT IN ('Njoftime për trashegimitarë' , 'Notification for inheritance' )`)


for (const row of rows) {
    pageQueue.add('default', { 
        actid: row.actid
    });
}


// for (let i = 2000; i <= 3000; i++) {
//     pageQueue.add( 'default', { 
//         actid: i
//     });
    
// }


// for (let i = 1; i <= 101000; i++) {
//     pageQueue.add('default', { 
//         actid: i
//     });
// }