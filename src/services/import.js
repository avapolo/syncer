const compressing = require('compressing');
const config = require('../config')
const fs = require('fs-extra');
const { nanoid } = require('nanoid');
const { Pool } = require("pg");
const objstore = require('./objstore')
const container = require('./container');
const logger = require('../logger');
const { waitForHealthy } = require('../db');

const db = new Pool(config.db)

const getLastImport = async() => {
  const sql = `SELECT max(iteration) FROM avapolos_sync WHERE instance='${config.instance}' AND operation='I'`;
  const result = (await db.query(sql)).rows[0].max;

  if (!result) return 0
  return result
}

const getNextImport = async() => {
  const lastImport = await getLastImport()
  return lastImport + 1;
}

const createControlRecord = async(iteration) => {
  const sql = `INSERT INTO avapolos_sync(instance,iteration,operation) VALUES ('${config.instance}', '${iteration}', 'I')`;
  await db.query(sql)
}

const resolveImportName = (instance, iteration) => `${instance}.${iteration}.tgz`;

const run = new Promise((resolve, reject) => {
  logger.debug('Starting import procedure');
  const nextImport = await getNextImport();
  logger.debug(`Next import iteration: ${nextImport}`);
  const importName = resolveImportName(config.instance, nextImport);
  logger.debug(`Searching for import filename: ${importName}`);
  const tmpPath = `/tmp/avapolos_syncer/${nanoid(8)}`;
  logger.debug(`temp path: ${tmpPath}`);
  const tmpImportPath = `${tmpPath}/import.tgz`;
  const tmpImportDataPath = `${tmpPath}/import`;
  const syncDataPath = await container.getVolumeMountpointByContainer(config.replication.sync, "/var/lib/postgresql/data");
  const moodledataFiledirPath = `${await container.getVolumeMountpointByContainer('moodle', "/app/moodledata")}/filedir`;

  if (! await objstore.has(config.minio.exportsBucket, importName)) throw new Error('import archive was not found');
  logger.debug('import archive was found on object storage');

  await objstore.get(config.minio.exportsBucket, importName, tmpImportPath);
  logger.debug('import archive was downloaded from object storage');
  await compressing.tgz.uncompress(tmpImportPath, tmpImportDataPath);
  logger.debug('import archive was decompressed');

  logger.debug('replacing sync database data');
  await fs.rmdir(syncDataPath, { recursive: true });
  await fs.copy(`${tmpImportDataPath}/database`, syncDataPath, { recursive: true });

  logger.debug('stopping main database')
  await container.stop(config.replication.main);
  logger.debug('starting sync database')
  await container.start(config.replication.sync);
  logger.debug('starting main database')
  await container.start(config.replication.main);
  
  logger.debug('waiting for db to be healthy')
  waitForHealthy().then(() => {
    logger.debug('waiting for replication to finish')
    await db.query("SELECT bdr.wait_slot_confirm_lsn(NULL, NULL)");
    logger.debug('stopping sync database')
    container.stop(config.replication.sync);
    logger.debug('creating control record')
    await createControlRecord(nextImport);
    logger.debug('purging moodle cache')
    await container.runCommand('moodle', ["php",  "/app/public/admin/cli/purge_caches.php"])
    logger.debug('restarting moodle')
    await container.restart('moodle');
    resolve()
  })
});

module.exports = {
  run
};
