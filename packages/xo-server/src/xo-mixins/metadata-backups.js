// @flow
import asyncMap from '@xen-orchestra/async-map'
import createLogger from '@xen-orchestra/log'
import defer from 'golike-defer'
import { fromEvent } from 'promise-toolbox'

import debounceWithKey from '../_pDebounceWithKey'
import parseDuration from '../_parseDuration'
import { type Xapi } from '../xapi'
import {
  noop,
  safeDateFormat,
  serializeError,
  type SimpleIdPattern,
  unboxIdsFromPattern,
} from '../utils'

import { type Executor, type Job } from './jobs'
import { type Schedule } from './scheduling'

const log = createLogger('xo:xo-mixins:metadata-backups')

const DIR_XO_CONFIG_BACKUPS = 'xo-config-backups'
const DIR_XO_POOL_METADATA_BACKUPS = 'xo-pool-metadata-backups'
const METADATA_BACKUP_JOB_TYPE = 'metadataBackup'

const compareTimestamp = (a, b) => a.timestamp - b.timestamp

type Settings = {|
  retentionXoMetadata?: number,
  retentionPoolMetadata?: number,
|}

type MetadataBackupJob = {
  ...$Exact<Job>,
  pools?: SimpleIdPattern,
  remotes: SimpleIdPattern,
  settings: $Dict<Settings>,
  type: METADATA_BACKUP_JOB_TYPE,
  xoMetadata?: boolean,
}

const createSafeReaddir = (handler, methodName) => (path, options) =>
  handler.list(path, options).catch(error => {
    if (error?.code !== 'ENOENT') {
      log.warn(`${methodName} ${path}`, { error })
    }
    return []
  })

// metadata.json
//
// {
//   jobId: String,
//   jobName: String,
//   scheduleId: String,
//   scheduleName: String,
//   timestamp: number,
//   pool?: <Pool />
//   poolMaster?: <Host />
// }
//
// File structure on remotes:
//
// <remote>
// ├─ xo-config-backups
// │  └─ <schedule ID>
// │     └─ <YYYYMMDD>T<HHmmss>
// │        ├─ metadata.json
// │        └─ data.json
// └─ xo-pool-metadata-backups
//    └─ <schedule ID>
//       └─ <pool UUID>
//          └─ <YYYYMMDD>T<HHmmss>
//             ├─ metadata.json
//             └─ data
//
// Task logs emitted in a metadata backup execution:
//
// job.start
// ├─ task.start(data: { type: 'pool', id: string, pool: <Pool />, poolMaster: <Host /> })
// │  └─ task.end
// ├─ task.start(data: { type: 'xo' })
// │  └─ task.end
// └─ job.end
export default class metadataBackup {
  _app: {
    createJob: (
      $Diff<MetadataBackupJob, {| id: string |}>
    ) => Promise<MetadataBackupJob>,
    createSchedule: ($Diff<Schedule, {| id: string |}>) => Promise<Schedule>,
    deleteSchedule: (id: string) => Promise<void>,
    getXapi: (id: string) => Xapi,
    getJob: (
      id: string,
      ?METADATA_BACKUP_JOB_TYPE
    ) => Promise<MetadataBackupJob>,
    updateJob: (
      $Shape<MetadataBackupJob>,
      ?boolean
    ) => Promise<MetadataBackupJob>,
    removeJob: (id: string) => Promise<void>,
  }

  get runningMetadataRestores() {
    return this._runningMetadataRestores
  }

  constructor(app: any, { backup }) {
    this._app = app
    this._logger = undefined
    this._runningMetadataRestores = new Set()

    const debounceDelay = parseDuration(backup.listingDebounce)
    this._listXoMetadataBackups = debounceWithKey(
      this._listXoMetadataBackups,
      debounceDelay,
      remoteId => remoteId
    )
    this.__listPoolMetadataBackups = debounceWithKey(
      this._listPoolMetadataBackups,
      debounceDelay,
      remoteId => remoteId
    )

    app.on('start', async () => {
      this._logger = await app.getLogger('metadataRestore')

      app.registerJobExecutor(
        METADATA_BACKUP_JOB_TYPE,
        this._executor.bind(this)
      )
    })
  }

  async _backupXo({ job, schedule, runJobId, handlers, retention }) {
    const app = this._app
    const logger = this._logger
    const timestampReg = /^\d{8}T\d{6}Z$/

    const timestamp = Date.now()
    const formattedTimestamp = safeDateFormat(timestamp)

    const taskId = logger.notice(`Starting XO metadata backup. (${job.id})`, {
      data: {
        type: 'xo',
      },
      event: 'task.start',
      parentId: runJobId,
    })

    try {
      const xoMetadataDir = `${DIR_XO_CONFIG_BACKUPS}/${schedule.id}`
      const dir = `${xoMetadataDir}/${formattedTimestamp}`

      const data = JSON.stringify(await app.exportConfig(), null, 2)
      const fileName = `${dir}/data.json`

      const metadata = JSON.stringify(
        {
          jobId: job.id,
          jobName: job.name,
          scheduleId: schedule.id,
          scheduleName: schedule.name,
          timestamp,
        },
        null,
        2
      )
      const metaDataFileName = `${dir}/metadata.json`

      await asyncMap(
        handlers,
        defer(async ($defer, { handler, id }) => {
          $defer.onFailure(() => handler.rmtree(dir))

          const subTaskId = logger.notice(
            `Starting XO metadata backup for the remote (${id}). (${job.id})`,
            {
              data: {
                id,
                type: 'remote',
              },
              event: 'task.start',
              parentId: taskId,
            }
          )

          try {
            await asyncMap([
              handler.outputFile(fileName, data),
              handler.outputFile(metaDataFileName, metadata),
            ])

            // deleting old backups
            await handler.list(dir).then(list => {
              list.sort()
              list = list
                .filter(timestamp => timestampReg.test(timestamp))
                .slice(0, -retention)
              return Promise.all(
                list.map(timestamp => {
                  const backupDir = `${dir}/${timestamp}`
                  return handler.rmtree(backupDir).catch(error => {
                    logger.warning(`unable to delete the folder ${backupDir}`, {
                      event: 'task.warning',
                      taskId,
                      data: {
                        error,
                      },
                    })
                  })
                })
              )
            }, noop)

            logger.notice(
              `Backuping XO metadata for the remote (${id}) is a success. (${
                job.id
              })`,
              {
                event: 'task.end',
                status: 'success',
                subTaskId,
              }
            )
          } catch (error) {
            logger.error(
              `Backuping XO metadata for the remote (${id}) has failed. (${
                job.id
              })`,
              {
                event: 'task.end',
                result: serializeError(error),
                status: 'failure',
                taskId: subTaskId,
              }
            )
            throw error
          }
        })
      )

      logger.notice(`Backuping XO metadata is a success. (${job.id})`, {
        event: 'task.end',
        status: 'success',
        taskId,
      })
    } catch (error) {
      logger.error(`Backuping XO metadata has failed. (${job.id})`, {
        event: 'task.end',
        result: serializeError(error),
        status: 'failure',
        taskId,
      })
      throw error
    }
  }

  async _backupPool(
    id,
    { handlers, job, runJobId, schedule, cancelToken, retention }
  ) {
    const app = this._app
    const logger = this._logger
    const timestampReg = /^\d{8}T\d{6}Z$/

    const timestamp = Date.now()
    const formattedTimestamp = safeDateFormat(timestamp)

    const xapi = this._app.getXapi(id)
    const poolMaster = await xapi.getRecord('host', xapi.pool.master)

    const taskId = logger.notice(
      `Starting metadata backup for the pool (${id}). (${job.id})`,
      {
        data: {
          id,
          pool: xapi.pool,
          poolMaster,
          type: 'pool',
        },
        event: 'task.start',
        parentId: runJobId,
      }
    )

    const poolMetadataDir = `${DIR_XO_POOL_METADATA_BACKUPS}/${
      schedule.id
    }/${id}`
    const dir = `${poolMetadataDir}/${formattedTimestamp}`

    // TODO: export the metadata only once then split the stream between remotes
    const stream = await app.getXapi(id).exportPoolMetadata(cancelToken)
    const fileName = `${dir}/data`

    const metadata = JSON.stringify(
      {
        jobId: job.id,
        jobName: job.name,
        pool: xapi.pool,
        poolMaster,
        scheduleId: schedule.id,
        scheduleName: schedule.name,
        timestamp,
      },
      null,
      2
    )
    const metaDataFileName = `${dir}/metadata.json`

    try {
      await asyncMap(
        handlers,
        defer(async ($defer, { handler, id }) => {
          $defer.onFailure(() => handler.rmtree(dir))

          const subTaskId = logger.notice(
            `Starting metadata backup for the pool (${id}) for the remote (${id}). (${
              job.id
            })`,
            {
              data: {
                id,
                type: 'remote',
              },
              event: 'task.start',
              parentId: taskId,
            }
          )

          try {
            await asyncMap([
              (async () => {
                const outputStream = await handler.createOutputStream(fileName)
                $defer.onFailure(() => outputStream.destroy())

                // 'readable-stream/pipeline' not call the callback when an error throws
                // from the readable stream
                stream.pipe(outputStream)
                return fromEvent(stream, 'end').catch(error => {
                  if (error.message !== 'aborted') {
                    throw error
                  }
                })
              })(),
              handler.outputFile(metaDataFileName, metadata),
            ])

            // deleting old backups
            await handler.list(dir).then(list => {
              list.sort()
              list = list
                .filter(timestamp => timestampReg.test(timestamp))
                .slice(0, -retention)
              return Promise.all(
                list.map(timestamp => {
                  const backupDir = `${dir}/${timestamp}`
                  return handler.rmtree(backupDir).catch(error => {
                    logger.warning(`unable to delete the folder ${backupDir}`, {
                      event: 'task.warning',
                      taskId,
                      data: {
                        error,
                      },
                    })
                  })
                })
              )
            }, noop)

            logger.notice(
              `Backuping pool metadata (${id}) for the remote (${id}) is a success. (${
                job.id
              })`,
              {
                event: 'task.end',
                status: 'success',
                subTaskId,
              }
            )
          } catch (error) {
            logger.error(
              `Backuping pool metadata (${id}) for the remote (${id}) has failed. (${
                job.id
              })`,
              {
                event: 'task.end',
                result: serializeError(error),
                status: 'failure',
                subTaskId,
              }
            )
            throw error
          }
        })
      )
    } catch (error) {
      logger.error(`Backuping pool metadata (${id}) has failed. (${job.id})`, {
        event: 'task.end',
        result: serializeError(error),
        status: 'failure',
        taskId,
      })
      throw error
    }
  }

  async _executor({
    cancelToken,
    job: job_,
    logger,
    runJobId,
    schedule,
  }): Executor {
    if (schedule === undefined) {
      throw new Error('backup job cannot run without a schedule')
    }

    const job: MetadataBackupJob = (job_: any)
    const remoteIds = unboxIdsFromPattern(job.remotes)
    if (remoteIds.length === 0) {
      throw new Error('metadata backup job cannot run without remotes')
    }

    const poolIds = unboxIdsFromPattern(job.pools)
    const isEmptyPools = poolIds.length === 0
    if (!job.xoMetadata && isEmptyPools) {
      throw new Error('no metadata mode found')
    }

    const { retentionXoMetadata = 0, retentionPoolMetadata = 0 } =
      job?.settings[schedule.id] || {}
    if (
      (!job.xoMetadata && retentionPoolMetadata === 0) ||
      (isEmptyPools && retentionXoMetadata === 0)
    ) {
      throw new Error('no retentions corresponding to the metadata modes found')
    }

    cancelToken.throwIfRequested()

    const app = this._app
    let handlers = await Promise.all(
      remoteIds.map(async id => {
        // TOTEST
        const handler = await app.getRemoteHandler(id).catch(error => {
          logger.warning(`unable to get the handler for the remote ${id}`, {
            event: 'task.warning',
            taskId: runJobId,
            data: {
              error,
            },
          })
        })
        return (
          handler && {
            id,
            handler,
          }
        )
      })
    )
    handlers = handlers.filter(_ => _ !== undefined)

    const promises = []
    if (job.xoMetadata && retentionXoMetadata !== 0) {
      promises.push(
        this._backupXo({
          handlers,
          job,
          retention: retentionXoMetadata,
          runJobId,
          schedule,
        })
      )
    }

    if (!isEmptyPools && retentionPoolMetadata > 0) {
      poolIds.forEach(id => {
        promises.push(
          this._backupPool(id, {
            cancelToken,
            handlers,
            job,
            retention: retentionPoolMetadata,
            runJobId,
            schedule,
          })
        )
      })
    }

    return asyncMap(promises)
  }

  async createMetadataBackupJob(
    props: $Diff<MetadataBackupJob, {| id: string |}>,
    schedules: $Dict<$Diff<Schedule, {| id: string |}>>
  ): Promise<MetadataBackupJob> {
    const app = this._app

    const job: MetadataBackupJob = await app.createJob({
      ...props,
      type: METADATA_BACKUP_JOB_TYPE,
    })

    const { id: jobId, settings } = job
    await asyncMap(schedules, async (schedule, tmpId) => {
      const { id: scheduleId } = await app.createSchedule({
        ...schedule,
        jobId,
      })
      settings[scheduleId] = settings[tmpId]
      delete settings[tmpId]
    })
    await app.updateJob({ id: jobId, settings })

    return job
  }

  async deleteMetadataBackupJob(id: string): Promise<void> {
    const app = this._app
    const [schedules] = await Promise.all([
      app.getAllSchedules(),
      // it test if the job is of type metadataBackup
      app.getJob(id, METADATA_BACKUP_JOB_TYPE),
    ])

    await Promise.all([
      app.removeJob(id),
      asyncMap(schedules, schedule => {
        if (schedule.id === id) {
          return app.deleteSchedule(id)
        }
      }),
    ])
  }

  // xoBackups
  // [{
  //   id: `${remoteId}/folderPath`,
  //   jobId,
  //   jobName,
  //   scheduleId,
  //   scheduleName,
  //   timestamp
  // }]
  async _listXoMetadataBackups(remoteId, handler) {
    const safeReaddir = createSafeReaddir(handler, 'listXoMetadataBackups')

    const backups = []
    await asyncMap(
      safeReaddir(DIR_XO_CONFIG_BACKUPS, { prependDir: true }),
      scheduleDir =>
        asyncMap(
          safeReaddir(scheduleDir, { prependDir: true }),
          async backupDir => {
            try {
              backups.push({
                id: `${remoteId}${backupDir}`,
                ...JSON.parse(
                  String(await handler.readFile(`${backupDir}/metadata.json`))
                ),
              })
            } catch (error) {
              log.warn(`listXoMetadataBackups ${backupDir}`, { error })
            }
          }
        )
    )

    return backups.sort(compareTimestamp)
  }

  // poolBackups
  // {
  //   [<Pool ID>]: [{
  //     id: `${remoteId}/folderPath`,
  //     jobId,
  //     jobName,
  //     scheduleId,
  //     scheduleName,
  //     timestamp,
  //     pool,
  //     poolMaster,
  //   }]
  // }
  async _listPoolMetadataBackups(remoteId, handler) {
    const safeReaddir = createSafeReaddir(handler, 'listXoMetadataBackups')

    const backupsByPool = {}
    await asyncMap(
      safeReaddir(DIR_XO_POOL_METADATA_BACKUPS, { prependDir: true }),
      scheduleDir =>
        asyncMap(safeReaddir(scheduleDir), poolId => {
          const backups = backupsByPool[poolId] ?? (backupsByPool[poolId] = [])
          return asyncMap(
            safeReaddir(`${scheduleDir}/${poolId}`, { prependDir: true }),
            async backupDir => {
              try {
                backups.push({
                  id: `${remoteId}${backupDir}`,
                  ...JSON.parse(
                    String(await handler.readFile(`${backupDir}/metadata.json`))
                  ),
                })
              } catch (error) {
                log.warn(`listPoolMetadataBackups ${backupDir}`, {
                  error,
                })
              }
            }
          )
        })
    )

    // delete empty entries and sort backups
    Object.keys(backupsByPool).forEach(poolId => {
      const backups = backupsByPool[poolId]
      if (backups.length === 0) {
        delete backupsByPool[poolId]
      } else {
        backups.sort(compareTimestamp)
      }
    })

    return backupsByPool
  }

  //  {
  //    xo: {
  //      [remote ID]: xoBackups
  //    },
  //    pool: {
  //      [remote ID]: poolBackups
  //    }
  //  }
  async listMetadataBackups(remoteIds: string[]) {
    const app = this._app

    const xo = {}
    const pool = {}
    await Promise.all(
      remoteIds.map(async remoteId => {
        try {
          const handler = await app.getRemoteHandler(remoteId)

          const [xoList, poolList] = await Promise.all([
            this._listXoMetadataBackups(remoteId, handler),
            this._listPoolMetadataBackups(remoteId, handler),
          ])
          if (xoList.length !== 0) {
            xo[remoteId] = xoList
          }
          if (Object.keys(poolList).length !== 0) {
            pool[remoteId] = poolList
          }
        } catch (error) {
          log.warn(`listMetadataBackups for remote ${remoteId}`, { error })
        }
      })
    )

    return {
      xo,
      pool,
    }
  }

  // Task logs emitted in a restore execution:
  //
  // task.start(message: 'restore', data: <Metadata />)
  // └─ task.end
  async restoreMetadataBackup(id: string) {
    const app = this._app
    const logger = this._logger
    const message = 'metadataRestore'
    const [remoteId, dir, ...path] = id.split('/')
    const handler = await app.getRemoteHandler(remoteId)
    const metadataFolder = `${dir}/${path.join('/')}`

    const taskId = logger.notice(message, {
      event: 'task.start',
      data: JSON.parse(
        String(await handler.readFile(`${metadataFolder}/metadata.json`))
      ),
    })
    try {
      this._runningMetadataRestores.add(taskId)

      let result
      if (dir === DIR_XO_CONFIG_BACKUPS) {
        result = await app.importConfig(
          JSON.parse(
            String(await handler.readFile(`${metadataFolder}/data.json`))
          )
        )
      } else {
        result = await app
          .getXapi(path[1])
          .importPoolMetadata(
            await handler.createReadStream(`${metadataFolder}/data`),
            true
          )
      }

      logger.notice(message, {
        event: 'task.end',
        result,
        status: 'success',
        taskId,
      })
    } catch (error) {
      logger.error(message, {
        event: 'task.end',
        result: serializeError(error),
        status: 'failure',
        taskId,
      })
      throw error
    } finally {
      this._runningMetadataRestores.delete(taskId)
    }
  }

  async deleteMetadataBackup(id: string) {
    const uuidReg = '\\w{8}(-\\w{4}){3}-\\w{12}'
    const metadataDirReg = 'xo-(config|pool-metadata)-backups'
    const timestampReg = '\\d{8}T\\d{6}Z'

    const regexp = new RegExp(
      `^/?${uuidReg}/${metadataDirReg}/${uuidReg}(/${uuidReg})?/${timestampReg}`
    )

    if (!regexp.test(id)) {
      throw new Error(`The id (${id}) not correspond to a metadata folder`)
    }
    const app = this._app
    const [remoteId, ...path] = id.split('/')

    const handler = await app.getRemoteHandler(remoteId)
    return handler.rmtree(path.join('/'))
  }
}
