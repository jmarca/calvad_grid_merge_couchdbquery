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
var test_db_unique = date.getHours()+'-'+date.getMinutes()+'-'+date.getSeconds()

var task,options
before(function(done){
    config_okay('test.config.json',function(err,c){
        options ={'couchdb':c.couchdb}
        options.couchdb.db += test_db_unique
        options.couchdb.statedb += test_db_unique

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options};
        task.cell_id= '178_92'
        task.year   = 2007
        async.each([options.couchdb.db,options.couchdb.statedb]
                  ,function(db,cb){
                       var cdb =
                           [task.options.couchdb.url+':'+task.options.couchdb.port
                           ,db].join('/')
                       superagent.put(cdb)
                       .type('json')
                       .auth(options.couchdb.auth.username
                            ,options.couchdb.auth.password)
                       .end(function(err,result){

                           cb()
                       })
                   }
                  ,done
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


describe('can mark as inprocess',function(){
    it('can mark a task and filter it out'
      ,function(done){
           in_process(task,function(err){
               should.not.exist(err)
               filter_grids(task,function(doit){
                   should.exist(doit)
                   task.state.should.have.length(3)
                   doit.should.not.be.ok;
                   return done()
               });
               return null
           });
           return null
       })
    it('does not filter out other tasks'
      ,function(done){
           task.cell_id= '178_91'
           task.state=[]
           filter_grids(task,function(doit){
                  should.exist(doit)
                   task.state.should.have.length(0)
                   doit.should.be.ok;
                   return done()
               });
               return null
           });
})
