// DynamoDB.js
// Find Getting Started AWS DynamoDB instructions at https://docs.aws.amazon.com/amazondynamodb/latest/gettingstartedguide/GettingStarted.Js.03.html

const aws = require('aws-sdk')
const chalk = require('chalk')

aws.config.accessKeyId = ''
aws.config.secretAccessKey = ''
aws.config.region = ''
const docClient = new aws.DynamoDB.DocumentClient()

const ResponseType = {
  ERROR: 'ERROR',
  SUCCESS: 'SUCCESS'
}

const updateReturnValues = {
  NONE: 'NONE',
  ALL_OLD: 'ALL_OLD',
  UPDATED_OLD: 'UPDATED_OLD',
  ALL_NEW: 'ALL_NEW',
  UPDATED_NEW: 'UPDATED_NEW'
}

function awsDynamoScan(params, callback) {
  const results = {}
  docClient.scan(params, (err, data) => {
    if (err) {
      console.log(chalk.bgRed(err))

      results.type = ResponseType.ERROR
      results.res = err
      return (callback && callback(results))

    }

    results.type = ResponseType.SUCCESS
    results.res = data
    return (callback && callback(results, err))

  })
}

function awsDynamoUpdate(params, callback) {
  const results = {}
  docClient.update(params, (err, data) => {
    if (err) {
      console.log(chalk.bgRed(err))

      results.type = ResponseType.ERROR
      results.res = err
      return (callback && callback(results))

    }

    results.type = ResponseType.SUCCESS
    results.res = data
    return (callback && callback(results))

  })
}

// Returns a formatted set of update parameters to input into awsDynamoUpdate...
// EXAMPLE RETURN VALUE:
//    {
//         TableName:'table',
//         Key:{
//             'year': year,
//             'title': title
//         },
//         UpdateExpression: 'set info.rating = :r, info.plot=:p, info.actors=:a',
//         ExpressionAttributeValues:{
//             ':r':5.5,
//             ':p':'Everything happens all at once.',
//             ':a':['Larry', 'Moe', 'Curly']
//         },
//         ReturnValues:'UPDATED_NEW'
//     }

function setUpdateParams(tableName, itemKeyName, itemKeyValue, valueDict, returnValues) {

  let updateExpression = 'set'
  const updateValues = {}
  const attributeNames = {}
  let count = 0

  for (const obj in valueDict) {

    if (count === 0) {
      updateExpression = updateExpression + ' #' + obj + ' = :' + obj
    } else {
      updateExpression = updateExpression + ', #' + obj + ' = :' + obj
    }
    updateValues[':' + obj] = valueDict[obj]
    attributeNames['#' + obj] = obj
    count += 1
  }

  const params = {}
  params.TableName = tableName
  params.Key = {}
  params.Key[itemKeyName] = itemKeyValue
  params.UpdateExpression = updateExpression
  params.ExpressionAttributeValues = updateValues
  params.ExpressionAttributeNames = attributeNames
  params.ReturnValues = returnValues

  return params

}

function awsDynamoQuery(params, callback) {
  const results = {}
  docClient.query(params, (err, data) => {
    if (err) {
      console.log(chalk.bgRed(err))

      results.type = ResponseType.ERROR
      results.res = err
      return (callback && callback(results))

    }

    results.type = ResponseType.SUCCESS
    results.res = data
    return (callback && callback(results))

  })
}

function awsDynamoPut(params, callback) {
  const results = {}
  docClient.put(params, (err, data) => {
    if (err) {
      console.log(chalk.bgRed(err))

      results.type = ResponseType.ERROR
      results.res = err
      return (callback && callback(results))

    }

    results.type = ResponseType.SUCCESS
    results.res = data
    return (callback && callback(results))

  })
}

function awsDynamoDelete(params, callback) {
  const results = {}
  docClient.delete(params, (err, data) => {
    if (err) {
      console.log(chalk.bgRed(err))

      results.type = ResponseType.ERROR
      results.res = err
      return (callback && callback(results))

    }

    results.type = ResponseType.SUCCESS
    results.res = data
    return (callback && callback(results))

  })
}

function scanForNumberOfItemsInATable(tableName, callback) {
  const params = {}
  params.TableName = tableName
  params.Select = 'COUNT'
  awsDynamoScan(params, (results) => {
    callback(results)
  })
}

function checkIfSingleItemExistsInTable(tableName, keyName, keyValue) {

  const promise = new Promise((resolve, reject) => {

    const params = {}
    params.TableName = tableName
    params.KeyConditionExpression = `${keyName} = :kValue`
    params.ExpressionAttributeValues = {
      ':kValue': keyValue
    }

  // console.log(params)

    awsDynamoQuery(params, (results) => {
      //console.log(results)

      if (results.type === ResponseType.SUCCESS) {

        if (results.res.Count > 0) {
          resolve(true)
        } else {
          resolve(false)
        }

      } else if (results.type === ResponseType.ERROR) {

        reject(results.res)

      }

    })

  })

  return promise

}

function queryForSingleItem(tableName, keyName, keyValue) {

  const promise = new Promise((resolve, reject) => {

    const params = {}
    params.TableName = tableName
    params.KeyConditionExpression = `${keyName} = :kValue`
    params.ExpressionAttributeValues = {
      ':kValue': keyValue
    }

    awsDynamoQuery(params, (results) => {
      //console.log(results)

      if (results.type === ResponseType.SUCCESS) {

        if (results.res.Count > 0) {
          resolve(results.res.Items[0])
        } else {
          reject()
        }

      } else if (results.type === ResponseType.ERROR) {

        reject(results.res)

      }

    })

  })

  return promise

}

exports.awsDynamoScan = awsDynamoScan
exports.awsDynamoUpdate = awsDynamoUpdate
exports.awsDynamoQuery = awsDynamoQuery
exports.awsDynamoPut = awsDynamoPut
exports.awsDynamoDelete = awsDynamoDelete
exports.setUpdateParams = setUpdateParams
exports.updateReturnValues = updateReturnValues
exports.scanForNumberOfItemsInATable = scanForNumberOfItemsInATable
exports.checkIfSingleItemExistsInTable = checkIfSingleItemExistsInTable
exports.queryForSingleItem = queryForSingleItem
