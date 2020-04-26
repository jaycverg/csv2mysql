const csv2mysql = require('../index')

// flag data
csv2mysql({
  source: './simple.csv',
  result: './data1.sql',
  tableName: 'users'
})

// with master data
csv2mysql({
  source: './simple.csv',
  result: './data2.sql',
  tableName: 'users',
  // we specified the column name becuase the fourth column is referenced by type id
  colNames: ['id', 'first_name', 'last_name', 'type_id'],
  masterData: {
    // zero-based, 3 means the fourth column
    3: {
      tableName: 'types',
      colNames: ['id', 'name']
    }
  }
})

// with one-to-many relation to master data
csv2mysql({
  source: './one-to-many.csv',
  result: './data3.sql',
  tableName: 'users',
  masterData: {
    3: {
      tableName: 'types',
      colNames: ['id', 'name'],
      many: true,
      valueDelimiter: /,/,
      xrefTableName: 'user_types',
      xrefCols: {
        user_id: (user) => user[0],
        type_id: (user, type) => type[0]
      }
    }
  }
})

