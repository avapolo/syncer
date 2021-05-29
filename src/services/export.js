const compressing = require('compressing');
const fs = require('fs-extra');
const { nanoid } = require('nanoid');

const { client:db, waitForHealthy } = require('../db');
const container = require('./container');
const objstore = require('./objstore');
const config = require('../config');
const logger = require('../logger');

const getNextExport = async() => {
  const lastExport = await getLastExport()
  return lastExport + 1;
}

const getLastExport = async() => {
  const sql = `SELECT max(iteration) FROM avapolos_sync WHERE instance='${config.instance}' AND operation='E'`;
  const result = (await db.query(sql)).rows[0].max;

  if (!result) return 0
  return result
}

const listByInstance = async (instance) => {
  const sql = `SELECT * FROM avapolos_sync WHERE instance='${instance}' AND operation='E';`
  const result = (await db.query(sql)).rows;

  return result
}

const resolveExportName = (instance, iteration) => `${instance}.${iteration}.tgz`;

// (C=clonagem, E=export, I=import)
const createControlRecord = async(iteration) => {
  const sql = `INSERT INTO avapolos_sync(instance,iteration,operation) VALUES ('${config.instance}', '${iteration}', 'E')`;
  await db.query(sql)
}

const copyDatabase = async (tmpPath) => {
  const source = await container.getVolumeMountpointByContainer(config.replication.main, "/var/lib/postgresql/data")
  const dest = `${tmpPath}/database`;

  await fs.copy(source, dest, { recursive: true, overwrite: true })
}

const copyMoodleDataFiledir = async (tmpPath) => {
  const source = `${(await container.getVolumeMountpointByContainer("moodle", "/app/moodledata"))}/filedir`
  const dest = `${tmpPath}/filedir`;

  await fs.copy(source, dest, { recursive: true, overwrite: true })
}

const createSyncPacket = async () => {
  const id = nanoid(6);
  const tmpPath = `/tmp/avapolos_syncer${id}`;
  const source = `${tmpPath}/`;
  const dest = `${__dirname}/${id}.tgz`;

  await copyDatabase(tmpPath)
  await copyMoodleDataFiledir(tmpPath)

  await compressing.tgz.compressDir(source, dest, { ignoreBase: true })
  await fs.rmdir(tmpPath, { recursive: true })

  return dest;
}

const run = async () => {
  try {
    logger.debug("Running export")
    logger.debug(`getting next export iteration`)
    const nextExport = await getNextExport();
    logger.debug(`next iteration is #${nextExport}`)
    logger.debug(`creating control record`);
    await createControlRecord(nextExport);
    logger.debug(`stopping main database`)
    await container.stop(config.replication.main);
    logger.debug(`creating sync packet`)
    const packetPath = await createSyncPacket()
    logger.debug(`uploading to object storage`);
    const ret = await objstore.put(config.minio.exportsBucket, resolveExportName(config.instance, nextExport), packetPath)
    logger.debug(ret)
    logger.debug("Removing local copy of sync packet")
    await fs.unlink(packetPath)
    
    logger.debug("cleaning queue")
    await container.start(config.replication.main);
    await container.start(config.replication.sync);

    logger.debug('waiting for BDR')

    waitForHealthy(async () => {
      await db.query("SELECT bdr.wait_slot_confirm_lsn(NULL, NULL)");
      logger.debug('stopping sync db')
      container.stop(config.replication.sync);
      return;
    })
    
  } catch(error) {
    logger.debug('an error ocurred, starting main db and stopping sync')
    await container.stop(config.replication.sync)
    await container.start(config.replication.main)
    logger.debug('done')
    throw error
  }
  
}

const getExportFromInstanceAndIteration = async (instance, iteration) => {
  return await objstore.getStream(config.minio.exportsBucket, resolveExportName(instance, iteration))
}

// const _main = async() => {
//   const res = await listByInstance("IES")
//   console.log(res)
// }

// _main()

module.exports = {
  getLastExport,
  getNextExport,
  resolveExportName,
  run,
  listByInstance,
  getExportFromInstanceAndIteration,
};
