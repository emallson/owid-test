const express = require('express');
const csv = require('csv-parse');
const Busboy = require('busboy');
const { Pool } = require('pg');
const RecordStore = require('./storage');

const db_pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: true,
});

const store = new RecordStore(db_pool);

const app = express();
app.post('/importCSV', (req, res) => {
  const busboy = new Busboy({ headers: req.headers });
  let totalFiles = 0;
  let processedFiles = 0;
  let finished = false;
  let sentErr = false;

  const finalize = () => res.status(200).send("Done.\n");

  busboy.on('file', (fieldname, file) => {
    totalFiles += 1;
    const parser = csv();
    let varIds = null;
    file.pipe(parser)
      .on('readable', async () => {
        let record;
        while(record = parser.read()) {
          try {
            if(varIds !== null) {
              await store.insert(record, varIds);
            } else {
              varIds = await store.getVariables(record);
            }
          } catch(err) {
            res.status(400).send(`Unable to fully process input "${fieldname}": ${err}; Record: ${record}\n`);
            sentErr = true;
          }
        }
      })
      .on('error', err => {
        res.status(400).send(`Unable to process input "${fieldname}": ${err}`);
        sentErr = true;
      })
      .on('finish', async () => {
        processedFiles += 1;
        if(!sentErr && finished && processedFiles === totalFiles) {
          await store.processQueue(true);
          finalize();
        }
      });
  });

  busboy.on('finish', () => { finished = true; });

  req.pipe(busboy);
});

app.get('/filter', async (req, res) => {
  try {
    const results = await store.query(req.query);

    res.status(200).send({
      results: results,
    });
  } catch (err) {
    console.error(err);
    res.status(400).send({
      err: err
    });
  }
});

const port = process.env.PORT;

app.listen(port, function() {
  console.log(`Running on port ${port}...`);
});
