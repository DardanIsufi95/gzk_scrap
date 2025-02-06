import { Queue  } from 'bullmq';
const connection = { host: '127.0.0.1', port: 6379 };


const pageQueue = new Queue('pageQueue', { connection });



// for (let i = 2000; i <= 3000; i++) {
//     pageQueue.add( 'default', { 
//         actid: i
//     });
    
// }


for (let i = 1; i <= 101000; i++) {
    pageQueue.add('default', { 
        actid: i
    });
}