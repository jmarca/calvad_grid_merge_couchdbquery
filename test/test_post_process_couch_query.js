/*global require before describe after it console */

// test for reduce.post_process_couch_query

// two things.  first make sure that it does what it is supposed to
// do, second make sure that the output is correct...that it doesn't
// break


// use the known months in test/files/*.json

// dump using the "to_csv" code I have in the modified formatter.js,
// then use a spreadsheet to figure out what the results *should* be.
// Then run the code on those same months and see if it works out
// properly.


var cdb_interactions = require('../lib/couchdb_interactions')
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var flatten_records = require('../lib/flatten').flatten_records
var reduce = require('../lib/reduce')
var config_okay = require('../lib/config_okay')
var queries = require('../lib/query_postgres')
var get_hpms_aadt = queries.get_hpms_from_sql
var get_detector_fractions = cdb_interactions.get_detector_fractions
var get_detector_routes = queries.get_detector_route_nums

var should = require('should')

var async = require('async')
var _ = require('lodash')
var config={}
var utils = require('./utils')
var superagent = require('superagent')
var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var task

before(function(done){
    config_okay('test.config.json',function(err,c){
        config.postgres=_.clone(c.postgres,true)
        config.couchdb =_.clone(c.couchdb,true)
        var date = new Date()
        var test_db_unique = date.getHours()+'-'
                           + date.getMinutes()+'-'
                           + date.getSeconds()+'-'
                           + date.getMilliseconds()

        config.couchdb.hpms_db += test_db_unique
        config.couchdb.detector_db += test_db_unique
        config.couchdb.state_db += test_db_unique
        return done()
    })
    return null
})

describe('post_process_hpms_couch_query',function(){
    before(utils.demo_db_before(config))
    after(utils.demo_db_after(config))

    it('should correctly post process a couch hpms result',function(done){
        var task ={'options':config
                  ,'cell_id':'100_223'
                  ,'year':2008
                  }
        task.should.not.have.property('scale')
        async.waterfall([
                     function(cb){
                         return get_hpms_fractions(task,cb)
                      }
                   ,function(t,cb){
                        return reduce.post_process_couch_query(t,cb)
                    }]
                    ,function(e,t){
                         should.not.exist(e)
                         should.exist(t)
                         t.should.have.property('scale')
                         t.scale.n.should.be
                         .approximately(1.0164381992, 0.00001)
                         t.scale.hh.should.be
                         .approximately(0.9912986681, 0.00001)
                         t.scale.nhh.should.be
                         .approximately(1.0068673223, 0.00001)
                         return done()
                     });

    })
    it('should correctly post process a couch detector result',function(done){
        var task ={'options':config
                  ,'cell_id':'189_72'
                  ,'year':2008
                  }
        task.should.not.have.property('scale')
        async.waterfall([
                     function(cb){
                         return get_detector_fractions(task,cb)
                      }
                   ,function(t,cb){
                        return reduce.post_process_couch_query(t,cb)
                    }]
                    ,function(e,t){
                         should.not.exist(e)
                         should.exist(t)
                         t.should.have.property('scale')
                         t.scale.n.should.be
                         .approximately(1.0, 0.00001)
                         t.scale.hh.should.be
                         .approximately(1.0, 0.00001)
                         t.scale.nhh.should.be
                         .approximately(1.0, 0.00001)
                         return done()
                     });

    })
})
