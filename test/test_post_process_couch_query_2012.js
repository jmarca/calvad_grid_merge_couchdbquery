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
var reduce = require('../lib/reduce')
var config_okay = require('config_okay')

var get_detector_fractions = cdb_interactions.get_detector_fractions


var should = require('should')

var queue = require('d3-queue').queue
var _ = require('lodash')
var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var superagent = require('superagent')
var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var options = {}
var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

before(function(done){
    config_okay(config_file,function(err,c){
        options.couchdb=c.couchdb
        options.couchdb.grid_merge_couchdbquery_hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_detector_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_state_db += test_db_unique

        utils.demo_db_before(options)(done)
        return null
    })
    return null
})
//after(utils.demo_db_after(options))

describe('post_process_hpms_couch_query',function(){

    it('should correctly post process a couch hpms result',function(done){
        var task ={'options':options
                  ,'cell_id':'100_223'
                  ,'year':2012
                  }
        task.should.not.have.property('scale')
        queue(1)
        .defer(get_hpms_fractions,task)
        .defer(reduce.post_process_couch_query,task)
        .await(function(e){
                         should.not.exist(e)
                         should.exist(task)
            task.should.have.property('scale')
                         task.scale.n.should.be
                         .approximately(1.1609, 0.0001)
                         task.scale.hh.should.be
                         .approximately(1.13207, 0.0001)
                         task.scale.nhh.should.be
                         .approximately(1.16378, 0.0001)
                         return done()
                     });

    })
    it('should correctly post process a couch detector result',function(done){
        var task ={'options':options
                  ,'cell_id':'128_172'
                  ,'year':2012
                  }
        task.should.not.have.property('scale')
        queue(1)
        .defer(get_detector_fractions,task)
        .defer(reduce.post_process_couch_query,task)
        .await(function(e){
                         should.not.exist(e)
                         should.exist(task)
                         task.should.have.property('scale')
                         task.scale.n.should.be
                         .approximately(1.0, 0.00001)
                         task.scale.hh.should.be
                         .approximately(1.0, 0.00001)
                         task.scale.nhh.should.be
                         .approximately(1.0, 0.00001)
                         return done()
                     });

    })
    it('should not crash on an empty cell',function(done){
        var task ={'options':options
                  ,'cell_id':'100_222'
                  ,'year':2009
                  }
        queue(1)
        .defer(get_detector_fractions,task)
        .defer(reduce.post_process_couch_query,task)
        .await(function(e){
            should.not.exist(e)
            should.exist(task)
            return done()
        });

    })
})
