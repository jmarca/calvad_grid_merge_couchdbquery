/* global require console process it describe after before */

var should = require('should')

var queue = require('d3-queue').queue
var _ = require('lodash')

var cdb_interactions = require('../lib/couchdb_interactions')
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
        options.couchdb.grid_merge_couchdbquery_hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_detector_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_state_db += test_db_unique

        utils.demo_db_before(options)(done)
        // // dummy up a done grid and a not done grid in a test db
        // task = {'options':options};
        // task.cell_id= '178_92'
        // task.year   = 2007
        // queue(1)
        // .defer(utils.create_tempdb
        //       ,task
        //       ,options.couchdb.grid_merge_couchdbquery_state_db)
        // .await(done)
        return null
    })
})

after(utils.demo_db_after(options))


describe('can mark as inprocess',function(){
    it('can mark a task and filter it out'
      ,function(done){
           task = {'options':options};
           task.cell_id= '178_92'
           task.year   = 2007

           in_process(task,function(err){
               should.not.exist(err)
               filter_out_done(task,function(err,doit){
                   should.not.exist(err)
                   should.exist(doit)
                   task.should.equal(doit)
                   task.state.should.have.length(3)
                   task.todo.should.not.be.ok;
                   return done()
               });
               return null
           });
           return null
       })
    // it('does not filter out other tasks'
    //   ,function(done){
    //        task.cell_id= '178_91'
    //        task.state=[]
    //        filter_out_done(task,function(doit){
    //               should.exist(doit)
    //                task.state.should.have.length(0)
    //                doit.should.be.ok;
    //                return done()
    //            });
    //            return null
    //        });
})
