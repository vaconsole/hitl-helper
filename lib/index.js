const Database = require('better-sqlite3')
const GenerateSchema = require('generate-schema')
const _ = require('lodash')
function initJob(db) {}

function jsonCreateTable(
  data = [],
  { pk = null, tableName = '_job_1', additionalColumns = {} } = {},
) {
  let SQL = `CREATE TABLE IF NOT EXISTS ${tableName} (`
  const {
    items: { properties },
  } = GenerateSchema.json('Job', data)
  const schema = Object.assign(properties, additionalColumns)
  Object.entries(schema).forEach(([key, { type, directive }], index) => {
    let sqlType = 'TEXT'
    if (type === 'string') sqlType = 'TEXT'
    if (type === 'number') sqlType = 'integer'
    if (key === pk) {
      sqlType = `${sqlType} primary key`
    }
    if (directive) {
      sqlType = `${sqlType} ${directive}`
    }
    SQL = `${SQL}${key} ${sqlType} ${
      index !== Object.entries(schema).length - 1 ? ',' : ''
    }`
  })
  return `${SQL});`
}

function insertJsonToTable(
  jsonArray = [],
  { db, tableName = 'tableName', upsert = false } = {},
) {
  let insertStatement = 'insert into'
  if (upsert) {
    insertStatement = 'insert or replace into'
  }
  jsonArray.forEach((data) => {
    const values = Object.values(data)
    const SQL = `${insertStatement} ${tableName} (${Object.keys(data).join(
      ',',
    )}) values (${Object.keys(values).fill('?').join(', ')})`
    db.prepare(SQL).run(...values)
  })
}

function createJob({ db, description = '', sql = '', pk = '' } = {}) {
  if (!pk) {
    throw new Error('need to specifiy primary key')
  }
  db.exec(`
    CREATE TABLE IF NOT EXISTS _job_meta (
            id integer NOT NULL,
            [status]  TEXT NOT NULL,
            [description] TEXT,
            [pk] TEXT NOT NULL,            
            [sql] TEXT NOT NULL,
            PRIMARY KEY (
                id
            )
        );
      `)
  const { lastInsertRowid } = db
    .prepare(
      'insert into _job_meta (description,sql,pk, status) values (?,?,?,?) returning rowid',
    )
    .run(description, sql, pk, 'init')
  const data = db.prepare(sql).all()
  const tableName = `_job_${lastInsertRowid}`
  const tableConfig = {
    tableName,
    additionalColumns: {
      _status: {
        type: 'string',
        directive: "DEFAULT 'init'",
      },
    },
    pk,
  }
  db.exec(jsonCreateTable(data, tableConfig))
  return lastInsertRowid
}

function updateJob({ db, job_id }) {
  const { sql } = db
    .prepare(`select * from _job_meta where id = '${job_id}'`)
    .get()
  const data = db.prepare(sql).all()
  const tableName = `_job_${job_id}`
  insertJsonToTable(data, { db, tableName, upsert: true })
}

function assignJob({ db, job_id }) {
  const tableName = `_job_${job_id}`
  db.prepare(
    `update ${tableName}
set _status = 'assigned'
where _status = 'init'`,
  ).run()
}

function getJob({ db, job_id }) {
  const tableName = `_job_${job_id}`
  return db
    .prepare(`select * from ${tableName} where _status = 'assigned'`)
    .all()
    .map((o) => _.omit(o, ['_status']))
}

function submitJob(inputData, { db, job_id = 1 } = {}) {
  const data = inputData.map((o) => _.omit(o, ['_status']))
  const { pk } = db
    .prepare(`select * from _job_meta where id = '${job_id}'`)
    .get()
  const tableName = `_job_${job_id}_submission`
  const tableConfig = {
    tableName,
    pk,
  }
  db.exec(jsonCreateTable(data, tableConfig))
  insertJsonToTable(data, { db, tableName, upsert: true })
  db.prepare(
    `update _job_${job_id}
  set _status = 'pending'
  where _status = 'assigned'`,
  ).run()
}

function acceptJob({ db, job_id }) {
  const data = db
    .prepare(`select * from _job_${job_id}_submission`)
    .all()
    .map((o) => _.omit(o, ['_status']))
  const { pk } = db
    .prepare(`select * from _job_meta where id = '${job_id}'`)
    .get()
  const tableConfig = {
    tableName: `_job_${job_id}_data`,
    pk,
  }
  db.exec(jsonCreateTable(data, tableConfig))
  insertJsonToTable(data, {
    db,
    tableName: `_job_${job_id}_data`,
    upsert: true,
  })
  db.prepare(
    `update _job_${job_id} 
set _status = 'done'
where _status = 'pending'`,
  ).run()
  db.prepare(`drop table _job_${job_id}_submission`).run()
}

function rejectJob({ db, job_id }) {
  db.prepare(
    `update _job_${job_id} 
set _status = 'assigned'
where _status = 'pending'`,
  ).run()
  db.prepare(`drop table _job_${job_id}_submission`).run()
}

module.exports = {
  initJob,
  createJob,
  jsonCreateTable,
  insertJsonToTable,
  getJob,
  updateJob,
  submitJob,
  acceptJob,
  assignJob,
  rejectJob,
}
