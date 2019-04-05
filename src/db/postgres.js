
import type { Database } from "../types";
import { promisify, formatTime } from "../utils";

const Promise = require('bluebird');
const initOptions = {
    promiseLib: Promise
};

// TODO: use any, none ...
const pgp = require('pg-promise')(initOptions);

//const url = process.env.POSTGRESQLDB_URI || "postgres://localhost:5433/postgres_data";
const pg_connection =
{
    host: 'localhost',
    port: 5433,
    database: 'postgres_data'
}

const connect = () => pgp(pg_connection);
  //promisify(pgp, "", url, { useNewUrlParser: true });

let dbPromise = null;
const getDB = () => {
  if (!dbPromise) {
    dbPromise = connect();
  }
  return dbPromise;
};

const init = async () => {
  const client = await getDB();
  await client.connect().catch(err => {
    console.log(`Failed to connect to db: ${err}`);
    process.exit(1)
  });
  //console.log(client);
  // TODO: use promisify ?
  // from_to = 'from' + '_' + 'to'
  // identifier = 'exchange' + _ + 'from' + '_' + 'to'

  let queryString =
    `CREATE TABLE IF NOT EXISTS "pairExchanges" (
      "from" VARCHAR(25),
      "to" VARCHAR(25),
      from_to VARCHAR(51),
      exchange VARCHAR(50),
      id VARCHAR(102) PRIMARY KEY,
      latest double precision,
      "latestDate" TIMESTAMPTZ,
      "yesterdayVolume" double precision,
      "oldestDayAgo" INTEGER,
      "hasHistoryFor1Year" BOOLEAN,
      "hasHistoryFor30LastDays" BOOLEAN,
      "historyLoadedAt_daily" TIMESTAMP,
      "historyLoadedAt_hourly" TIMESTAMP,
      histo_daily JSONB,
      histo_hourly JSONB
      )`;

  const resPairs = await client.query(queryString);
  console.log("Table Schema Created for pairExchanges");

  queryString =
  `CREATE TABLE IF NOT EXISTS marketcap_coins (
    day TIMESTAMP,
    coins VARCHAR(25)[]
  )`;
  const resMarketcap = await client.query(queryString);
  console.log("Table Schema Created for marketcap_coins");

  queryString =
  `CREATE TABLE IF NOT EXISTS meta (
    id VARCHAR(50) PRIMARY KEY,
    "lastMarketCapSync" TIMESTAMPTZ,
    "lastLiveRatesSync" TIMESTAMPTZ
  )`;

  const resMeta = await client.query(queryString);
  console.log("Table Schema Created for meta");
};

const metaId = "meta_1";

async function setMeta(meta) {
  if (!meta) {
    return;
  }
  const client = await getDB();
  const argsStringBis = ['?id'].concat(Object.keys(meta).map(item => {return {name : `${item}`, cast: 'TIMESTAMPTZ'}}));
  const cs = new pgp.helpers.ColumnSet(argsStringBis, {table: 'meta'});
  const value = {
    id : metaId,
    ... meta
  };
  const update = pgp.as.format('UPDATE SET ($1^) = $2^', [
        cs.names, pgp.helpers.values(value, cs)
  ]);
  const query = pgp.helpers.insert([value], cs) + ' ON CONFLICT(id) DO ' + update;
  await client.any(query).catch(err => process.exit(1));
}

async function getMeta() {
  const client = await getDB();
  const meta = await client.query(`SELECT * FROM meta WHERE metaId = ${metaId}`);
  return {
    lastLiveRatesSync: new Date(0),
    lastMarketCapSync: new Date(0),
    ...meta
  };
}

async function statusDB() {
  const client = await getDB();
  const count = await client.query(`SELECT COUNT(*) FROM "pairExchanges"`);
  if (count === 0) throw new Error("database is empty");
}

async function updateLiveRates(all) {
  console.log(">>> Start updateLiveRates");
  const client = await getDB();
  const cs = new pgp.helpers.ColumnSet(['?id', 'latest', {name : 'latestDate', cast: 'TIMESTAMPTZ'}], {table: 'pairExchanges'});
  const values = all.map(item => {
    return {
      id : item.pairExchangeId,
      latest: item.price,
      latestDate: `${(new Date()).toISOString()}`
    };
  });
  const query = pgp.helpers.update(values, cs) + ` WHERE v.id = t.id`;
  await client.any(query).catch(err => process.exit(1));
  console.log("<<< Finish updateLiveRates");
  await setMeta({ lastLiveRatesSync: new Date() });
}

async function updateHisto(id, granurity, histo) {
  console.log(">>> Start updateHisto");
  const client = await getDB();
  await client.any(`UPDATE "pairExchanges" SET histo_${granurity} = $1 WHERE id = '${id}'`, [histo]).catch(err => {
    console.log(err);
    process.exit(1);
  });
  console.log("<<< Finish updateHisto");
}

async function updateExchanges(exchanges) {
  console.log(">>> Start updateExchanges");
  const client = await getDB();
  const cs = new pgp.helpers.ColumnSet(['?id', 'exchange'], {table: 'pairExchanges'});
  const values = exchanges.map(item => {
    return {
      id : item.id,
      exchange: item.name,
    };
  });
  const query = pgp.helpers.update(values, cs) + ` WHERE v.id = t.id`;
  await client.any(query).catch(err => process.exit(1));
  console.log("<<< Finish updateExchanges");
}

async function insertPairExchangeData(pairExchanges) {
  // TODO: use pgp.helpers.ColumnSet ? https://github.com/vitaly-t/pg-promise/wiki/Data-Imports
  // TODO: use massive insert massive-insert ?
  // Nothing to insert then ...
  if (pairExchanges.length === 0) {
    return;
  }
  console.log(">>> Start insertPairExchangeData");
  const client = await getDB();
  const argsStringBis = Object.keys(pairExchanges[0]);
  const cs = new pgp.helpers.ColumnSet(argsStringBis, {table: 'pairExchanges'});
  const upsert = pgp.helpers.insert(pairExchanges, cs) + ' ON CONFLICT(id) DO UPDATE SET ' + cs.assignColumns({from: 'EXCLUDED', skip: ['id']});
  await client.any(upsert);
  console.log("<<< Finish insertPairExchangeData");
}

async function updatePairExchangeStats(id, stats) {
  // Nothing to update then ...
  if (!stats) {
    return;
  }
  console.log(">>> Start updatePairExchangeStats");
  const client = await getDB();
  const cs = new pgp.helpers.ColumnSet(Object.keys(stats), {table: 'pairExchanges'});
  const query = pgp.helpers.update(stats, cs) + ` WHERE id = UPPER('${id}')`;
  await client.any(query);
  console.log("<<< Finish updatePairExchangeStats");
}

async function updateMarketCapCoins(day, coins) {
  console.log(">>> Start updateMarketCapCoins");
  const client = await getDB();
  await client.any(`UPDATE marketcap_coins SET coins = $2 WHERE day = '${day}'`, [`${day}`, coins]).catch(err => {
    console.log(err);
    process.exit(1);
  });
  await setMeta({ lastMarketCapSync: new Date() });
}

// This seems not to be used
async function queryExchanges() {
  throw new Error('queryExchanges Not Implemented');
}

const queryPairExchangesSort = coll =>
  coll.sort((a, b) => {
    const histoDiff =
      Number(b.hasHistoryFor1Year) - Number(a.hasHistoryFor1Year);
    if (histoDiff !== 0) return histoDiff;
    return b.yesterdayVolume - a.yesterdayVolume;
  });

async function queryPairExchangesByPairs(pairs) {
  console.log(' >>> Start queryPairExchangesByPairs(pairs)');
  const client = await getDB();
  const finalPairs = pairs.map(p => `${p.from + "_" + p.to}`.toUpperCase());
  const docs = await client.query(`SELECT * FROM "pairExchanges" WHERE id in ($1)`,[finalPairs]);
  console.log(' <<< Finish queryPairExchangesByPairs(pairs)');
  return docs;
}

async function queryPairExchangesByPair(pair, opts = {}) {
  console.log(' >>> Start queryPairExchangesByPairs');
  const client = await getDB();
  const { from, to } = pair;
  const query = 'SELECT * FROM "pairExchanges" WHERE from = $1 AND to = $2';
  let docs = await (opts.filterWithHistory ? client.query(`${query} AND hasHistoryFor30LastDays = TRUE`,[from, to]) : client.query(`${query}`,[from, to]));
  console.log(' <<< Finish queryPairExchangesByPairs');
  return docs;
}

async function queryPairExchangeIds() {
  console.log(' >>> Start queryPairExchangeIds');
  const client = await getDB();
  const idArray = await client.query(`SELECT id FROM "pairExchanges"`);
  console.log('<<< Finish queryPairExchangeIds');
  return idArray.map(id => id.id);
}

const queryPairExchangeById = async (id, projection) => {
  console.log(' >>> Start queryPairExchangeById');
  const client = await getDB();
  // TODO: why do we use projection here ?
  const doc = await client.query(`SELECT * FROM "pairExchanges" WHERE id = UPPER('${id}')`);
  console.log('<<< Finish queryPairExchangeById');
  if (!doc && doc.length > 0) {
    console.log(` !! No exchange for ${id}`);
    return;
  }
  return doc[0];
};

const queryMarketCapCoinsForDay = async day => {
  console.log(' >>> Start queryMarketCapCoinsForDay');
  const client = await getDB();
  const coins = await client.query(`SELECT coins from marketcap_coins WHERE day = '${day}'`).catch((err) => {
    console.log(err);
    process.exit(1)
  });
  console.log('<<< Finish queryMarketCapCoinsForDay');
  return coins;
};

const database: Database = {
  init,
  getMeta,
  statusDB,
  updateLiveRates,
  updateHisto,
  updateExchanges,
  insertPairExchangeData,
  updatePairExchangeStats,
  updateMarketCapCoins,
  queryExchanges,
  queryPairExchangeIds,
  queryPairExchangesByPairs,
  queryPairExchangesByPair,
  queryPairExchangeById,
  queryMarketCapCoinsForDay
};

export default database;
