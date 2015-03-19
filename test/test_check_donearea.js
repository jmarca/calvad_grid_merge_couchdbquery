var checker = require('couch_check_state')
var should = require('should')

var queue = require('queue-async')
var _ = require('lodash')

var cdb_interactions = require('../.')
var filter_out_done = cdb_interactions.filter_out_done
var mark_done = cdb_interactions.mark_done
var in_process = cdb_interactions.mark_in_process

var fs = require('fs')
var superagent=require('superagent')

var config_okay = require('config_okay')

var date = new Date()
var test_db_unique = date.getHours()+'-'+date.getMinutes()+'-'+date.getSeconds()

var task,options={}
var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

before(function(done){
    config_okay(config_file,function(err,c){
        options.couchdb=c.couchdb
        options.couchdb.grid_merge_couchdbquery_put_db += test_db_unique
        task = {options:c}

        var db = task.options.couchdb.grid_merge_couchdbquery_put_db
        var q = queue(1)
        q.defer(utils.create_tempdb,task,db)
        q.await(function(e){
            should.not.exist(e)
            queue(1)
                .defer(utils.load_area_sums,task)
                .await(done)
            return null
        })
        return null
    })
})

after(function(done){
    var db = task.options.couchdb.grid_merge_couchdbquery_put_db
    utils.delete_tempdb(task,db,function(e,r){
        return done()
    })
    return null
})

describe('check_results_doc function',function(){
    it('should find a doc that is there',function(done){
        var _task = _.clone(task)
        task.doc = {"id":"airbasin_NORTH COAST_2007"}
        cdb_interactions.check_results_doc(task,function(err,result){
            should.not.exist(err)
            should.exist(result)
            result.should.be.ok;
            return done()
        })
        return null
    })
    it('should not find a doc that is not there',function(done){
        var _task = _.clone(task)
        task.doc = {"id":"airbasin_SAN FRANCISCO BAY AREA_2007"}
        cdb_interactions.check_results_doc(task,function(err,result){
            should.not.exist(err)
            should.exist(result)
            result.should.not.be.ok;
            return done()
        })
        return null
    })
    return null
})
