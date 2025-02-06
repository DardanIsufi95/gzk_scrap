// server.js
import express from 'express';
import { Queue } from 'bullmq';
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js';
import { ExpressAdapter } from '@bull-board/express';




const connection = { host: '127.0.0.1', port: 6379 };


const pageQueue = new Queue('pageQueue', { connection });


const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/');


createBullBoard({
  queues: [new BullMQAdapter(pageQueue)],
  serverAdapter,
});


const app = express();
app.use('/', serverAdapter.getRouter());


app.get('/', (req, res) => {
  res.redirect('/');
});

const port = process.env.PORT || 3001;
app.listen(port, () => {
  console.log(`Bull Board dashboard is running at http://localhost:${port}`);
});