const { Pool } = require("pg");

const config = require('../config');

const db = new Pool(config.db)

const init = async () => {
  const sql = `
  CREATE TABLE IF NOT EXISTS avapolos_sync (
    id serial not null PRIMARY KEY,
    created timestamptz not null DEFAULT NOW(),
    instance char(4) not null,
    iteration int not null,
    operation char(1) not null
  );
  `;

  try {
    await db.query(sql);
  } catch (e) {
    // This error occurs
    if (e.code === "55P03") return;
    throw e;
  }
}

const waitForHealthy = (retries) => new Promise((resolve, reject) => {
  const sql = 'SELECT * FROM avapolos_sync';
  const delay = 1000;
  const retryCount = retries || 10;

  try {
    await db.query(sql);
    resolve()
  } catch {
    setTimeout(() => waitForHealthy(retryCount - 1), delay)
  }
})

module.exports = {
  client: db,
  waitForHealthy,
  init,
};
