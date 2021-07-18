#!/usr/bin/env node
const program = require('commander')
const Database = require('better-sqlite3')
const { init, populate, match } = require('../lib/index')

program.command('init <dbPath>').action((dbPath) => {
  const db = new Database(dbPath)
  init(db)
})

program
  .command('populate <dbPath> <table> <pk>')
  .action((dbPath, table, pk) => {
    const db = new Database(dbPath)
    populate(db, table, pk)
  })

program
  .command('match <dbPath> <table> <indexColumns>')
  .action((dbPath, table, indexColumns) => {
    const db = new Database(dbPath)
    match(db, table, indexColumns.split(','))
  })

program.parse(process.argv)
