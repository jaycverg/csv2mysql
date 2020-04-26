const fs = require('fs')
const csv = require('csv-parser')


const createSql = (tableName, colNames, rows) => `
insert into
\`${tableName}\` (${colNames.map(c => `\`${c}\``).join(', ')})
values
${rows.map(row => `(${row.map(c => `"${c}"`).join(', ')})`).join(',\n')}
;
`


// main function
module.exports = async (config) => {

  const masterDataConfig = config.masterData || {}

  const rows = []
  const colNames = config.colNames
  const masterData = {}
  const xrefTables = {}

  const processHeaders = headers => {
    if (!config.colNames) {
      config.colNames = headers
    }

    if (config.excludeCols && config.excludeCols.length > 0) {
      config.colNames = config.colNames
        .filter((c, i) => config.excludeCols.indexOf(i) < 0)
    }
  }

  const findOrInsert = (table, value) => {
    if (value in table) {
      return table[value]
    } else {
      const id = (table.__size || 0) + 1
      table[value] = id
      table.__size = id
      return id
    }
  }

  const processMasterData = (tableMeta, rowEntries, colIndex) => {
    const table = masterData[tableMeta.tableName] || {}
    const colValue = rowEntries[colIndex]
    if (tableMeta.many === true) {
      const values = colValue.split(tableMeta.valueDelimiter)
      const xrefTable = xrefTables[tableMeta.xrefTableName] || []
      for (let value of values) {
        value = value.trim()
        const id = findOrInsert(table, value)
        const xrefRow = []
        for (const xrefColMeta of Object.values(tableMeta.xrefCols)) {
          xrefRow.push(xrefColMeta(rowEntries, [id, value]))
        }
        xrefTable.push(xrefRow)
      }
      xrefTables[tableMeta.xrefTableName] = xrefTable
    } else {
      rowEntries[colIndex] = findOrInsert(table, colValue)
    }
    masterData[tableMeta.tableName] = table
  }

  const processLine = row => {
    const rowEntries = Object.values(row)

    for (let i = 0; i < rowEntries.length; ++i) {
      const tableMeta = masterDataConfig[i]
      if (tableMeta) {
        processMasterData(tableMeta, rowEntries, i)
      }
    }

    if (config.excludeCols && config.excludeCols.length > 0) {
      rows.push(rowEntries.filter((e, i) => config.excludeCols.indexOf(i) < 0))
    } else {
      rows.push(rowEntries)
    }
  }

  const generateSql = () => {
    fs.writeFileSync(config.result, '')
    const masterDataMeta = Object.values(masterDataConfig)
    for (const meta of masterDataMeta) {
      const table = masterData[meta.tableName]
      delete table.__size

      const rows = []
      for (const key in table) {
        rows.push([table[key], key])
      }

      const sql = createSql(meta.tableName, meta.colNames, rows)
      fs.appendFileSync(config.result, sql)
    }

    for (const meta of masterDataMeta) {
      if (meta.many !== true) {
        continue
      }

      const table = xrefTables[meta.xrefTableName]
      const sql = createSql(meta.xrefTableName, Object.keys(meta.xrefCols), table)
      fs.appendFileSync(config.result, sql)
    }

    const sql = createSql(config.tableName, config.colNames, rows)
    fs.appendFileSync(config.result, sql)
  }

  // also exclude columns that are marked as one-to-many
  config.excludeCols = config.excludeCols || []
  for (const key in masterDataConfig) {
    const meta = masterDataConfig[key]
    if (meta.many === true) {
      config.excludeCols.push(+key)
    }
  }

  return new Promise((resolve, reject) => {
    fs.createReadStream(config.source)
      .pipe(csv({}))
      .on('headers', processHeaders)
      .on('data', processLine)
      .on('end', () => {
        generateSql()
        resolve()
      })
      .on('error', error => reject(error))
  })

}
