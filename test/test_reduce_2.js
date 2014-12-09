/* global require console process it describe after before */

var should = require('should')

var queue = require('queue-async')
var _ = require('lodash')

var reduce = require('../lib/reduce')
var config_okay = require('config_okay')


var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var utils = require('./utils')
var superagent = require('superagent')
var cdb_interactions = require('../lib/couchdb_interactions')
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var get_detector_fractions = cdb_interactions.get_detector_fractions

var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var options={}
var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

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

after(utils.demo_db_after(options))



describe('post process couch query',function(){
    it('should compute scale of not 1,1,1 for hpms data',function(done){
        var task ={'options':options
                  ,'cell_id':'100_223'
                  ,'year':2008
                  }
        queue(1)
        .defer(get_detector_fractions,task)
        .defer(get_hpms_fractions,task)
        .defer(reduce.post_process_couch_query,task)
        .await(function(e,t1,t2,t3){
            should.not.exist(e)
            should.exist(task)
            task.should.have.property('scale')
            task.scale.should.have.keys('n','hh','nhh')
            task.scale.n.should.not.eql(1)
            task.scale.hh.should.not.eql(1)
            task.scale.nhh.should.not.eql(1)
            return done()
        })
        return null;
    })}
)
