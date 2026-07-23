'use strict';

const Redis = require('ioredis');
const { Client } = require('pg');

// Same connection settings the original .NET worker + docker-compose.yml use.
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const PG_HOST = process.env.PG_HOST || 'db';
const PG_USER = process.env.PG_USER || 'postgres';
const PG_PASSWORD = process.env.PG_PASSWORD || 'postgres';
const PG_DATABASE = process.env.PG_DATABASE || 'postgres';

const VOTES_LIST_KEY = 'votes';
const BLOCK_TIMEOUT_SECONDS = 5; // how long BLPOP waits before looping again

let shuttingDown = false;
let pgClient = null;

function log(...args) {
  console.log(new Date().toISOString(), '-', ...args);
}

// ---------- Postgres ----------

async function connectPostgres() {
  while (!shuttingDown) {
    const client = new Client({
      host: PG_HOST,
      user: PG_USER,
      password: PG_PASSWORD,
      database: PG_DATABASE,
    });

    // If the connection dies later (network blip, db restart) we want to know
    // about it so the main loop can reconnect instead of silently hanging.
    client.on('error', (err) => {
      log('Postgres connection error:', err.message);
      pgClient = null;
    });

    try {
      await client.connect();
      await client.query(`
        CREATE TABLE IF NOT EXISTS votes (
          id   VARCHAR(255) NOT NULL UNIQUE,
          vote VARCHAR(255) NOT NULL
        )
      `);
      log('Connected to db');
      return client;
    } catch (err) {
      log('Waiting for db -', err.message);
      await sleep(1000);
    }
  }
  return null;
}

async function updateVote(client, voterId, vote) {
  // ON CONFLICT upsert is the Node equivalent of the original insert-then-
  // update-on-failure logic, and relies on the same UNIQUE(id) constraint.
  await client.query(
    `INSERT INTO votes (id, vote) VALUES ($1, $2)
     ON CONFLICT (id) DO UPDATE SET vote = EXCLUDED.vote`,
    [voterId, vote]
  );
}

// ---------- Redis ----------

function connectRedis() {
  const redis = new Redis({
    host: REDIS_HOST,
    retryStrategy: (times) => Math.min(times * 200, 2000),
    maxRetriesPerRequest: null, // needed so BLPOP isn't aborted while redis is reconnecting
  });

  redis.on('connect', () => log('Connected to redis'));
  redis.on('error', (err) => log('Redis connection error:', err.message));

  return redis;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------- Main loop ----------

async function main() {
  log('Worker starting');

  const redis = connectRedis();
  pgClient = await connectPostgres();

  while (!shuttingDown) {
    let popped;
    try {
      // BLPOP blocks (up to BLOCK_TIMEOUT_SECONDS) instead of busy-polling
      // every 100ms like the original worker did.
      popped = await redis.blpop(VOTES_LIST_KEY, BLOCK_TIMEOUT_SECONDS);
    } catch (err) {
      log('Error popping from redis:', err.message);
      await sleep(1000);
      continue;
    }

    if (!popped) {
      // Timed out with nothing on the list - just loop again (this is our
      // keep-alive tick, same purpose as the original's `SELECT 1`).
      if (pgClient) {
        try {
          await pgClient.query('SELECT 1');
        } catch (err) {
          log('DB keep-alive failed, will reconnect:', err.message);
          pgClient = null;
        }
      }
      if (!pgClient) {
        pgClient = await connectPostgres();
      }
      continue;
    }

    const [, json] = popped;
    let vote;
    try {
      vote = JSON.parse(json);
    } catch (err) {
      log('Skipping malformed vote payload:', json);
      continue;
    }

    log(`Processing vote for '${vote.vote}' by '${vote.voter_id}'`);

    if (!pgClient) {
      log('Reconnecting DB');
      pgClient = await connectPostgres();
    }

    try {
      await updateVote(pgClient, vote.voter_id, vote.vote);
    } catch (err) {
      log('Error writing vote to db, will reconnect:', err.message);
      pgClient = null;
    }
  }

  await redis.quit();
  if (pgClient) await pgClient.end();
}

process.on('SIGTERM', () => {
  log('SIGTERM received, shutting down');
  shuttingDown = true;
});
process.on('SIGINT', () => {
  log('SIGINT received, shutting down');
  shuttingDown = true;
});

main().catch((err) => {
  console.error('Fatal worker error:', err);
  process.exit(1);
});