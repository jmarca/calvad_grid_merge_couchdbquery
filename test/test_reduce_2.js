/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var reduce = require('../lib/reduce')
var config_okay = require('../lib/config_okay')
var queries = require('../lib/query_postgres')
var get_hpms_aadt = queries.get_hpms_from_sql
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
        return done()
    })
    return null
})

var utils = require('./utils')
var superagent = require('superagent')
var cdb_interactions = require('../lib/couchdb_interactions')
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var get_detector_fractions = cdb_interactions.get_detector_fractions

describe('post process couch query',function(){
    var task
    before(function(done){
        var options = _.clone(config,true)
        options.couchdb.hpms_db += test_db_unique
        options.couchdb.detector_db += test_db_unique
        options.couchdb.state_db += test_db_unique

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options}
        async.each([task.options.couchdb.detector_db
                   ,task.options.couchdb.hpms_db
                   ,task.options.couchdb.state_db]
                  ,function(db,cb){
                       task.options.couchdb.db=db
                       utils.create_tempdb(task,cb)
                       return null
                   }
                  ,function(){
                       task.options.couchdb.db=null
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
        async.each([task.options.couchdb.detector_db
                   ,task.options.couchdb.hpms_db
                   ,task.options.couchdb.state_db]
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

    it('should compute scale of not 1,1,1 for hpms data',function(done){
        async.waterfall([function(cb){
                             var local_task = _.clone(task)
                             local_task.cell_id='100_223'
                             local_task.year=2008
                             return cb(null,local_task)
                         }
                        ,get_detector_fractions
                        ,get_hpms_fractions
                        ,function(t,cb){
                             reduce.post_process_couch_query(t,function(e,tt){
                                 should.not.exist(e)
                                 should.exist(tt)
                                 tt.should.have.property('scale')
                                 tt.scale.should.have.keys('n','hh','nhh')
                                 tt.scale.n.should.not.eql(1)
                                 tt.scale.hh.should.not.eql(1)
                                 tt.scale.nhh.should.not.eql(1)
                                 return done()
                             })
                             return null
                         }]
                       ,function(e,t){
                            should.not.exist(e)
                            should.exist(t)
                            return done()
                        })
        return null;
    })}
)
