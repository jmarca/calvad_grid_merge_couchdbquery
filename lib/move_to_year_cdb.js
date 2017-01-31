/**
 * @fileOverview move data in couchdb to year by year databases
 *
 * The purpose of this script is to methodically move all documents in
 * the carb/grid/state4k and carb/grid/state4k/hpms databases into
 * year by year databases.  This will put a maximum upper bound on
 * each database because once the year is done, no more data will get
 * entered into that DB.  In contrast, the current approach of putting
 * all the data in a single database means that the database can grow
 * without bound, and also means that every time a new year of data is
 * added, all of the views to old data will stall while processing the
 * new data.  This can take days, when it doesn't need to be the case
 * at all because the view is computed for static data and should be
 * done done done.
 *
 * Logic (at initial development time) is to query a bunch of
 * documents, look at each one and assign to a hash by date, then
 * delete the documents from the database and write the original docs
 * to the correct by-year database.  This should be done for one
 * database at a time (carb/grid/state4k, then
 * carb/grid/state4k/hpms). It is assumed that the databases already
 * exist, because it is only a handful and they're easy to make.
 *
 * all design documents will not be deleted from the original DB, and
 * will be written to *all* known databases (years) when those
 * databases are noticed (a new year is added to the data hash)
 *
 * @name move_to_year_cdb.js
 * @author James E. Marca <james@activimetrics.com>
 * @license GPL v2
 */
"use strict"
var setter = require('couch_set_state')
var getter = require('couchdb_get_views')
var putter = require('couchdb_put_doc')
var bulkdoc_saver = require('couchdb_bulkdoc_saver')
var _ = require('lodash')
var queue = require('d3-queue').queue
var check_if_year_db = require('./couchdb_year_check.js')
var superagent = require('superagent')

/**
 * get document count - get the number of documents in a couchdb database
 * @param {Object} config - the configuration object
 * @param {Object} config.couchdb - how to access couchdb
 * @param {String} config.couchdb.host - the database host.  Required
 * @param {String} config.couchdb.port - the database port.  Required
 * @param {String} config.couchdb.db - the database to query for
 * document count.  Required
 * @param {Function} cb - a callback function, expecting an error as
 * first argument (if any) and a number as the second argument
 * corresponding to the number of documents in the database.
 * @returns {}
 */
function get_doc_count (config,db,cb){
    if(typeof db === 'function'){
        throw new Error('db required as second argument')
    }


    var cdb = config.couchdb.host+':'+config.couchdb.port+'/'+db
    if(! /http/.test(cdb)){
        cdb = 'http://'+cdb
    }
    superagent.get(cdb).type('json')
        .end(function(e,r){
            var body
            if(e){ return cb(e,null) }
            body = JSON.parse(r.text)
            return cb(null,+body.doc_count)
        })
    return null
}

/**
 * get_some_docs - get 1000 docs from couchdb
 * @param {Object} config - the configuration object
 * @param {Object} config.couchdb - how to access couchdb
 * @param {String} config.couchdb.host - the database host.  Required
 * @param {String} config.couchdb.port - the database port.  Required
 * @param {String} config.couchdb.db - the database to query for
 * document count.  Required
 * @param {Function} cb - a callback function, expecting an error as
 * first argument, and a object containing a list of documents as
 * second argument.  That object will have one keys including
 * 'original', which contains documents suitable for sending to
 * bulkdocs to delete the docs, and then one key per year seen.  If
 * the docs are all in one year, then you'll only get one key aside
 * from 'original'
 *
 * @returns {} null
 */
function get_some_docs(config,db, cb){
    if(typeof db === 'function'){
        throw new Error('db required as second argument')
    }

    var opts = _.assign({},config.couchdb
                        ,{'db':db
                          ,'view':'_all_docs'
                          ,'include_docs':true
                          ,'reduce':false
                          ,'limit':config.limit || 1000})
    getter(opts
           ,function(e,r){
               var doclist = {'original':[]}
               if(e !== undefined){
                   return cb(e,null)
               }
               if(r !== undefined
                  && r.rows !== undefined
                  && r.rows.length>0){
                   // return documents to caller loop over docs and
                   // make a minor edit if necessary the current model
                   // version expects aadt_frac.  the old version said
                   // plain aadt, which is misleading as it is really
                   // an aadt fraction.  So if I see aadt and not
                   // aadt_frac, move aadt to aadt_frac.  But only for
                   // hpms.  Leave non-hpms alone?
                   r.rows.forEach(function(d,i){
                       var doc = d.doc
                       var year
                       var ts_match = /(\\d*)-0?(\\d*)-0?(\\d*)/.exec(doc.ts)
                       if(ts_match !== undefined){
                           year = ts_match[1]
                       }else{
                           console.log('no ts match for doc',doc)
                           return null
                       }
                       // double check document aadt thing
                       if(doc.aadt_frac === undefined){
                           if(doc.aadt === undefined ){
                               console.log('bad aadt_frac check for doc',doc)
                               return null
                           }
                           doc.aadt_frac = doc.aadt
                           delete doc.aadt
                       }
                       if(doclist[year] === undefined){
                           doclist[year]=[doc]
                       }else{
                           doclist[year].push(doc)
                       }
                       doclist.original.push({"_id":doc._id
                                              ,"_rev":doc._rev
                                              ,"_deleted":true})
                       return null
                   })
                   // have populated doclist, return it to caller
                   return cb(null,doclist)
               }else{
                   return cb(null,[])
               }
           })
    return null
}

function move_data(config,dbname,cb){
    var docsmoved = 0
    // loop, getting docs and moving them, until no more docs to get
    var q = queue(1)
    q.defer(get_some_docs,config,dbname)
    q.await(function(e,r){
        var q2 = queue(1)
        // r is an object.  if empty, all done
        if(r.original === undefined){
            // bail
            return cb(null,docsmoved)
        }
        // else, got stuff, so deal with it
        Object.keys(r).forEach(function(yr){
            if (yr === 'original'){
                // this is a delete, deal with it last
                return null
            }else{
                q2.defer(bulkdoc_saver,_.extend({},config.couchdb,{docs:r.original}))

        // first, do the deletes

        q2.defer(bulkdoc_saver,_.extend({},config.couchdb,{docs:r.original}))

}

function move_to_years(config,cb){
    // move data
    var q = queue(2)
    var dbs = [config.couchdb.grid_merge_couchdbquery_detector_db
               ,config.couchdb.grid_merge_couchdbquery_hpms_db
              ]
    q.defer(move_data,config,dbs[0])
    q.defer(move_data,config,dbs[1])
    q.await(cb)
}

module.exports = move_to_years
