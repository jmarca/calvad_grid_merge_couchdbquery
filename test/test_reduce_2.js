/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var reduce = require('../lib/reduce')
var config_okay = require('../lib/config_okay')
var queries = require('../lib/query_postgres')
var get_hpms = queries.get_hpms_from_sql
var get_detector_routes = queries.get_detector_route_nums


var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var config
before(function(done){
    config_okay('test.config.json',function(err,c){
        config ={'postgres':c.postgres
                ,'couchdb':c.couchdb}
        console.log(config)
        return done()
    })
    return null
})

var utils = require('./utils')
var superagent = require('superagent')
var cdb_interactions = require('../lib/couchdb_interactions')
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var get_detector_fractions = cdb_interactions.get_detector_fractions

describe('merge hourly fractions and AADT',function(){
    var task
    before(function(done){
        var options = _.clone(config,true)
        options.couchdb.db += test_db_unique
        options.couchdb.statedb += test_db_unique

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options}
        var datadb = task.options.couchdb.db
        async.each([task.options.couchdb.db,task.options.couchdb.statedb]
                  ,function(db,cb){
                       task.options.couchdb.db=db
                       utils.create_tempdb(task,cb)
                       return null
                   }
                  ,function(){
                       task.options.couchdb.db=datadb
                       async.series([function(cb){
                                           utils.load_hpms(task,cb)
                                           return null
                                       }
                                      ,function(cb){
                                           utils.load_detector(task,cb)
                                           return null
                                       }]
                                     ,done)
                   }
                  );
        return null
    })
    after(function(done){
        async.each([task.options.couchdb.db,task.options.couchdb.statedb]
                  ,function(db,cb){
                       var cdb =
                           [task.options.couchdb.url+':'+task.options.couchdb.port
                           ,db].join('/')
                       superagent.del(cdb)
                       .type('json')
                       .auth(task.options.couchdb.auth.username
                            ,task.options.couchdb.auth.password)
                       .end(cb)
                   }
                  ,function(){
                       done()
                   });
        return null

    })

    it('should apply fractions from detector-based grid',function(done){
        async.waterfall([function(cb){
                             var local_task = _.clone(task)
                             local_task.options.couchdb.detector_data_db=task.options.couchdb.db
                             local_task.cell_id='189_72'
                             local_task.year=2008
                             return cb(null,local_task)
                         }
                        ,get_detector_fractions
                        ,get_hpms
                        ,get_detector_routes
                        ,reduce.post_process_sql_queries
                        ,function(t,cb){
                             reduce.apply_fractions(t,function(err,cb_task){
                                 should.not.exist(err)
                                 should.exist(cb_task)
                                 return cb()
                             })
                             return null
                         }]
                       ,function(e,t){
                            should.not.exist(e)
                            should.exist(t)
                            return done()
                        })
        return null;
    })
})
