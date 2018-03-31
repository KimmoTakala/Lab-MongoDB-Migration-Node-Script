const fs = require('fs')
const mongodb= require('mongodb')
const async = require('async');

const url = 'mongodb://localhost:27017'
const customerCollection = 'customer-data'
const addressCollection = 'address-data'

mongodb.MongoClient.connect(url, (error, dbConnection) => {
    if (error) {
        console.log('Failed to connect to ' + url)
        return process.exit(1)
    } 
  
    let db = dbConnection.db('customers')
    db.dropDatabase(function(error, result){
        insertCustomers(dbConnection, db)
    });    
})

function insertCustomers(dbConnection, db) {
    console.log('Inserting customers to database')

    const customerDataFile = './m3-customer-data.json'
    var customerData = fs.readFileSync(customerDataFile)
    if (!customerData) {
        console.log('Failed to read ' + customerDataFile)
        process.exit(1)
    }
    var jsonCustomers = JSON.parse(customerData)

    db.collection(customerCollection).insertMany(jsonCustomers, (error, result) => {
        if (error) {
            console.log('Failed to insert customer data to database: ' + error)
            return process.exit(1)
        }
        insertAddresses(dbConnection, db);
    })
}

function insertAddresses(dbConnection, db) {
    console.log('Inserting addresses to database')
    
    const addressDataFile = './m3-customer-address-data.json'
    var addressData = fs.readFileSync(addressDataFile)
    if (!addressData) {
        console.log('Failed to read ' + addressData)
        process.exit(1)
    }
    var jsonAddresses = JSON.parse(addressData)

    db.collection(addressCollection).insertMany(jsonAddresses, (error, result) => {
        if (error) {
            console.log('Failed to insert address data to database: ' + error)
            return process.exit(1)
        }
        getRowCount(dbConnection, db)
  })   
}

function getRowCount(dbConnection, db) {
    console.log('Getting customers row count')
    
    db.collection(customerCollection).count((error, dbRowCount) => {
        if (error) {
            console.log('Failed to get customers row count: ' + error)
            return process.exit(1)
        }
        startMergeTasks(dbConnection, db, dbRowCount)
    })    
}

function startMergeTasks(dbConnection, db, dbRowCount) {
    console.log('Creating merge tasks')
    
    var taskRowCount
    if (process.argv.length > 2) {
        taskRowCount = parseInt(process.argv[2]);    
    }
    if (!taskRowCount)
    {
        taskRowCount = 50
    }
    var taskCount = Math.floor(dbRowCount / taskRowCount)
    console.log('Database row count is ' + dbRowCount + ', using ' + taskCount + ' tasks')
    
    var lastTaskRowCount = dbRowCount - ((taskCount - 1) * taskRowCount)
    console.log('lastTaskRowCount is ' + lastTaskRowCount)
    var tasks = []
    for (var i=0; i < taskCount; i++) {
        const startRow = i * taskRowCount
        const rowCount = i == taskCount - 1 ? lastTaskRowCount : taskRowCount
        
        tasks.push(function(callback){
            readCustomerRows(db, startRow, rowCount, callback)
            });        
    }

    async.parallel(tasks, (error, results) => {
        if (error) {
            console.error(error)
        } else {
            console.log('merge completed successfully')
        }
        dbConnection.close()
    })
}

function readCustomerRows(db, startRow, maxRows, callback) {
    var endRow = startRow + maxRows
    console.log('Merging rows from ' + startRow + ' to ' + endRow)

    db.collection(customerCollection)
    .find({})
    .skip(startRow)
    .limit(maxRows)
    .toArray((error, customerRows) => {
        if (error) {
            console.log('Failed to get customers from database: ' + error)
            return process.exit(1)
        }
        readAddressRows(db, startRow, maxRows, customerRows, callback)
    })
}

function readAddressRows(db, startRow, maxRows, customerRows, callback) {
    db.collection(addressCollection)
    .find({})
    .skip(startRow)
    .limit(maxRows)
    .toArray((error, addressRows) => {
        if (error) {
            console.log('Failed to get addresses from database: ' + error)
            return process.exit(1)
        }
        mergeRows(db, customerRows, addressRows, callback)
    })
}

function mergeRows(db, customerRows, addressRows, callback) {
    var rowCount = customerRows.length
    var mergeCount = 0
    for (var i=0; i < rowCount; i++) {
        db.collection(customerCollection)
        .update({ id: customerRows[i].id }, 
        {$set: 
            {
                "phone": addressRows[i].phone
              , "city": addressRows[i].city
              , "state": addressRows[i].state
              , "country": addressRows[i].country
            }
        }, 
        (error, results) => {
          if (error) {
            console.log('Failed to update data: ' + error)
            return process.exit(1)
          }
          mergeCount++
          if (mergeCount == rowCount) {
            callback()
          }
        }
      )
    }
}