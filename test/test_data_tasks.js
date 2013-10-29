/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var cdb_interactions = require('../lib/couchdb_interactions')
var filter_grids = cdb_interactions.filter_out_done
var mark_done = cdb_interactions.mark_done
var in_process = cdb_interactions.mark_in_process

var get_detector_routes = require('../lib/query_postgres').get_detector_route_nums
var fs = require('fs')
var superagent=require('superagent')

var config_okay = require('../lib/config_okay')

var date = new Date()
var test_db_unique = date.getHours()+'-'+date.getMinutes()+'-'+date.getSeconds()+'-'+date.getMilliseconds()

var task,options

function create_tempdb(task,cb){
    var cdb =
        [task.options.couchdb.url+':'+task.options.couchdb.port
        ,task.options.couchdb.db].join('/')
    superagent.put(cdb)
    .type('json')
    .auth(options.couchdb.auth.username
         ,options.couchdb.auth.password)
    .end(function(err,result){
        cb()
    })
}

function load_hpms(task,cb){
    var db_dump = require('./files/100_223_2008_JAN.json')

    var docs = _.map(db_dump.rows
                    ,function(row){
                         return row.doc
                     })
    var cdb = [task.options.couchdb.url+':'+task.options.couchdb.port
              ,task.options.couchdb.db].join('/')
    var couch =  cdb
    superagent.post(couch+'/_bulk_docs')
    .type('json')
    .send({"docs":docs})
    .end(function(e,r){
        return cb(e)
    })
}

function load_detector(task,cb){
    var db_dump = require('./files/189_72_2008_JAN.json')
    var docs = _.map(db_dump.rows
                    ,function(row){
                         return row.doc
                     })
    var cdb = [task.options.couchdb.url+':'+task.options.couchdb.port
              ,task.options.couchdb.db].join('/')
    var couch = cdb
    superagent.post(couch+'/_bulk_docs')
    .type('json')
    .send({"docs":docs})
    .end(function(e,r){
        return cb(e)
    })
}

before(function(done){
    config_okay('test.config.json',function(err,c){
        options ={'couchdb':c.couchdb}
        options.couchdb.db += test_db_unique
        options.couchdb.statedb += test_db_unique

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options};
        var datadb = options.couchdb.db
        async.each([options.couchdb.db,options.couchdb.statedb]
                  ,function(db,cb){
                       task.options.couchdb.db=db
                       create_tempdb(task,cb)
                       return null
                   }
                  ,function(){
                       task.options.couchdb.db=datadb
                       console.log('created dbs')
                       async.series([function(cb){
                                           load_hpms(task,cb)
                                           return null
                                       }
                                      ,function(cb){
                                           load_detector(task,cb)
                                           return null
                                       }]
                                     ,done)
                   }
                  );
        return null
    })
})
after(function(done){
        async.each([options.couchdb.db,options.couchdb.statedb]
                  ,function(db,cb){
                       var cdb =
                           [task.options.couchdb.url+':'+task.options.couchdb.port
                           ,db].join('/')
                       superagent.del(cdb)
                       .type('json')
                       .auth(options.couchdb.auth.username
                            ,options.couchdb.auth.password)
                       .end(cb)
                   }
                  ,function(){
                       done()
                   });
    return null

})


describe('get hpms fractions',function(){

    it('can get data for a known grid')

    it('will not crash if an unkown grid is passed in')


})
