const concord = require('../lib/index')
const fs = require('fs')
const Database = require('better-sqlite3')
const dbPath = 'test/input/input.db'
let db = null
beforeEach(() => {
  const initSql = fs.readFileSync('test/input/init.sql', 'utf-8')
  try {
    fs.unlinkSync(dbPath)
  } catch (error) {}
  // db = new Database(dbPath, { verbose: console.log })
  db = new Database(dbPath)
})

afterEach(() => {})

test('jsonCreateTable', () => {
  const data = [
    {
      a: 'test',
    },
    {
      b: 1,
    },
  ]

  const SQL = concord.jsonCreateTable(data, { tableName: 'job_1' })
  db.exec(SQL)
  db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([{ a: 'test', b: 1 }])
})

test('jsonCreateTable additional Columns', () => {
  const data = [
    {
      a: 'test',
    },
    {
      b: 1,
    },
  ]
  const tableConfig = {
    tableName: 'job_1',
    additionalColumns: {
      test: {
        type: 'string',
        directive: "DEFAULT 'test'",
      },
      test1: {
        type: 'number',
      },
    },
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  console.log(result)
  expect(result).toEqual([{ a: 'test', b: 1, test: 'test', test1: null }])
})

test('jsonCreateTable rowId', () => {
  const data = [
    {
      a: 'test',
      rowid: 1,
    },
    {
      b: 1,
      rowid: 2,
    },
  ]

  const SQL = concord.jsonCreateTable(data, { tableName: 'job_1', pk: 'rowid' })
  db.exec(SQL)
  db.exec(`insert into job_1 (a,rowid) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([{ a: 'test', rowid: 1, b: null }])
})

test('insertJsonToTable basic', () => {
  const data = [
    {
      a: 'test',
    },
    {
      b: 1,
    },
  ]
  const tableConfig = {
    tableName: 'job_1',
  }
  const insertConfig = {
    db,
    tableName: 'job_1',
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  concord.insertJsonToTable(data, insertConfig)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: 'test', b: null },
    { a: null, b: 1 },
  ])
})

test('insertJsonToTable multiple', () => {
  const data = [
    {
      a: 'test',
      b: 2,
    },
    {
      b: 1,
    },
  ]
  const tableConfig = {
    tableName: 'job_1',
  }
  const insertConfig = {
    db,
    tableName: 'job_1',
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  concord.insertJsonToTable(data, insertConfig)
  // db.exec(SQL)
  // db.exec(`insert into job_1 (a,b) values ('test',1)`)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: 'test', b: 2 },
    { a: null, b: 1 },
  ])
})

test('insertJsonToTable upserts', () => {
  let data = [
    {
      a: '2',
      b: 2,
    },
    {
      a: '1',
      b: 1,
    },
  ]
  const tableConfig = {
    tableName: 'job_1',
    pk: 'b',
  }
  const insertConfig = {
    db,
    tableName: 'job_1',
    upsert: true,
  }
  const SQL = concord.jsonCreateTable(data, tableConfig)
  db.exec(SQL)
  concord.insertJsonToTable(data, insertConfig)

  data = [
    {
      a: '4',
      b: 2,
    },
    {
      a: '3',
      b: 1,
    },
    { a: '4', b: 3 },
  ]

  concord.insertJsonToTable(data, insertConfig)
  const result = db.prepare('select * from job_1').all()
  expect(result).toEqual([
    { a: '3', b: 1 },
    { a: '4', b: 2 },
    { a: '4', b: 3 },
  ])
})

test('createJob', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  concord.createJob(job)
  const result = db.prepare('select * from _job_1').all()
  console.log(result)
  // expect(result).toEqual([
  //   { id: 'a1', ref: 'b1', _task_id: 1, _status: 'init' },
  //   { id: 'a2', ref: 'b1', _task_id: 2, _status: 'init' },
  //   { id: 'a3', ref: 'c1', _task_id: 3, _status: 'init' },
  // ])
})

test('update_one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'init' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'init' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'init' },
  ])
})

test('multiple_update', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  concord.updateJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  console.log(result)
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'init' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'init' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'init' },
  ])
})

test('assign_one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  concord.assignJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'assigned' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'assigned' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'assigned' },
  ])
})

test('getJob_one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  concord.assignJob({ db, job_id })
  const result = concord.getJob({ db, job_id })
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1' },
    { rowid: 2, id: 'a2', ref: 'b1' },
    { rowid: 3, id: 'a3', ref: 'c1' },
  ])
})

test('submit_job_one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  concord.assignJob({ db, job_id })
  const data = [
    { rowid: 1, id: 'a1', ref: 'c' },
    { rowid: 2, id: 'a2', ref: 'c' },
    { rowid: 3, id: 'a3', ref: 'c' },
  ]
  concord.submitJob(data, { db, job_id })
  let result = db.prepare('select * from _job_1_submission').all()
  expect(result).toEqual(data)
  result = db.prepare('select * from _job_1').all()
  console.log(result)
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'pending' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'pending' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'pending' },
  ])
})

test('accept_job_one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  concord.assignJob({ db, job_id })
  const data = [
    { rowid: 1, id: 'a1', ref: 'c' },
    { rowid: 2, id: 'a2', ref: 'c' },
    { rowid: 3, id: 'a3', ref: 'c' },
  ]
  concord.submitJob(data, { db, job_id })
  concord.acceptJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'done' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'done' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'done' },
  ])
  const output = db.prepare('select * from _job_1_data').all()
  console.log(output)
  expect(output).toEqual([
    { rowid: 1, id: 'a1', ref: 'c' },
    { rowid: 2, id: 'a2', ref: 'c' },
    { rowid: 3, id: 'a3', ref: 'c' },
  ])
})

test('reject_job_one', () => {
  db.exec(fs.readFileSync('test/input/init_2.sql', 'utf-8'))
  const job = {
    description: 'test job',
    pk: 'rowid',
    sql: 'select rowid,* from a',
    db,
  }
  const job_id = concord.createJob(job)
  concord.updateJob({ db, job_id })
  concord.assignJob({ db, job_id })
  const data = [
    { rowid: 1, id: 'a1', ref: 'c' },
    { rowid: 2, id: 'a2', ref: 'c' },
    { rowid: 3, id: 'a3', ref: 'c' },
  ]
  concord.submitJob(data, { db, job_id })
  concord.rejectJob({ db, job_id })
  const result = db.prepare('select * from _job_1').all()
  expect(result).toEqual([
    { rowid: 1, id: 'a1', ref: 'b1', _status: 'assigned' },
    { rowid: 2, id: 'a2', ref: 'b1', _status: 'assigned' },
    { rowid: 3, id: 'a3', ref: 'c1', _status: 'assigned' },
  ])
})
