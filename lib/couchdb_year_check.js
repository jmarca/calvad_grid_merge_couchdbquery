var superagent = require('superagent')

/**
 * check_if_year_db
 *
 * check if there is a database in couchdb that exists if you attach a
 * year onto the end of the task.options.couchdb.db value.  The year
 * is extracted from the task.ts (timestamp) value.
 *
 * This function is needed because it makes it easier to break data up
 * into year-specific databases.  The original approach was to put all
 * data into a single db.  But over time the database gets very large.
 * By splitting data up into years, it is somewhat easier to manage
 * the big database files.  The downside is that you have to make sure
 * to write all the necessary views to each of the year by year
 * databases.
 *
 * @param {Object} task - the task object, with details about couchdb connection
 * @param {String} task.ts - the timestamp of the request that is
 * coming, if year isn't passed
 * @param {Number} task.year - the year, if ts isn't passed
 * @param {Object} task.options - the various db access options in the task
 * @param {Object} task.options.couchdb -  the couchdb options
 * @param {Object} task.options.couchdb.db - the database root.  This
 * function will add the year taken from the task.ts value, and test
 * if the resulting database exists.
 * @param {Function} cb - the result of the couchdb test will be
 * passed to this function.  cb should accept error, task.  Task will
 * have modified task.options.couchdb.db to either include the year if
 * the database exists, or not.
 * @returns {null}
 * @throws {Error} - if it can't parse a year from task.ts, it will
 * throw.  if task.options.couchdb.host is not defined, it will throw.
 * if task.options.couchdb.db is not defined, it will throw.
 */
function check_if_year_db(task,cb){
    var date = task.ts
    var year = task.year
    if(year === undefined && date !== undefined){
        var year_result = /(\d\d\d\d)/.exec(task.ts)
        if(!year_result){
            throw new Error('no year in date '+date)
        }
        year = year_result[1]
    }
    if(year === undefined){
        throw new Error('need either a valid year param, or else a year defined in a ts param')
    }
    var c = task.options.couchdb
    var cdb = c.host
    if(cdb === undefined){
        throw new Error('no host defined in config file for couchdb')
    }
    var db = c.db
    if(db === undefined){
        throw new Error('no db defined in config file for couchdb')
    }
    var cdb_year_result = /2f(\d\d\d\d)$/.exec(db)

    // the following if statement checks if the year is already part
    // of the couchdb name.  if it is, and if the year in the name
    // matches the year in the time stamp, then don't bother pinging
    // couchdb to see if the database exists.  Instead, just return
    // with the current db name unchanged...we're already using the
    // year.
    //
    // This is so that I can just stack these checks in a loop of
    // reads or writes without paying the penalty of a call to couchdb
    // every time
    if(cdb_year_result){
        if( +cdb_year_result[1] !== year ){
            console.log(cdb_year_result)
            // year is already in db name, but not the same year, so croak
            throw new Error('year mismatch.  db is set as '+db+' but checking for '+year)
        }else{
            // already using the correct year, don't waste time
            // checking if it exists
            return cb(null,task)
        }
    }

    // if still here, then year isn't part of db name, so add it in
    // and check if it exists with an async call to couchdb
    var cport = c.port || 5984
    cdb = cdb+':'+cport
    if(! /http/.test(cdb)){
        cdb = 'http://'+cdb
    }


    var year_couchdb = cdb+'/'+db+'%2f'+year
    // if the year_couchdb exists, then change task.couchdb.db to
    // include the year

    // need to use request or superagent or whatever to ping the
    // possible couchdb.  if the db exists, then good. if not, then
    // bad
    console.log(year_couchdb)
    superagent.get(year_couchdb).type('json')
        .end(function(err,result){
            if(err && err.status && +err.status == 404){
                return cb(null,task)
            }
            if(err){
                return (err,null)
            }
            task.options.couchdb.db = db +'%2f'+year
            cb(null,task)
            return null
        })
    return null
}
module.exports = check_if_year_db
