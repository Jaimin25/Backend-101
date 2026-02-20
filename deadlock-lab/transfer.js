const { Client } = require('pg');

const { Pool } = require('pg');

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'deadlock_lab',
  password: 'postgres',
  port: 5432,
  max: 50   // allow up to 50 concurrent connections
});

const sleep = (ms) => new Promise(res => setTimeout(res, ms));

async function transfer(from, to, amount) {
    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        await client.query(
            'UPDATE accounts SET balance = balance - $1 WHERE id = $2',
            [amount, from]
        );

        await sleep(200);

        await client.query(
            'UPDATE accounts SET balance = balance + $1 WHERE id = $2',
            [amount, to]
        );

        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }
}
async function runConcurrentTransfers() {
    const tasks = [];

    for (let i = 0; i < 50; i++) {
        const from = Math.random() > 0.5 ? 1 : 2;
        const to = from === 1 ? 2 : 1;

        tasks.push(
            runWithRetry(from, to, 100)
                .catch(err => {
                    console.error('Final failure:', err.message);
                })
        );
    }

    await Promise.all(tasks);

    console.log('All transfers completed');
}
(async () => {
    await runConcurrentTransfers();
    await pool.end();
})();

async function runWithRetry(from, to, amount, retries = 10) {
    console.log('running?')
    for (let i = 0; i < retries; i++) {
        try {
            await transfer(from, to, amount);
            return; 
        } catch (err) {
            console.log(err.code);
            
            if (err.code === '40P01') {
                const delay = Math.pow(2, i) * 100;
                console.log(`Deadlock detected. Retrying in ${delay}ms`);
                await sleep(delay);
            } else {
                throw err;
            }
        }
    }

    throw new Error('Max retries exceeded');
}

    const from = parseInt(process.argv[2]);
    const to = parseInt(process.argv[3]);

    runWithRetry(from, to, 100)