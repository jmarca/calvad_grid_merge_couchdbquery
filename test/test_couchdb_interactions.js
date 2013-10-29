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

var task,options,created_locally=false;
before(function(done){
    config_okay('test.config.json',function(err,c){
        options ={'couchdb':c.couchdb}
        options.couchdb.db.should.eql('testdb')
        options.couchdb.statedb.should.eql('teststatedb')

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options};
        task.cell_id= '178_92'
        task.year   = 2007
        async.each([options.couchdb.db,options.couchdb.statedb]
                  ,function(db,cb){
                       var cdb =
                           [task.options.couchdb.url
                           ,db].join('/')
                       console.log(cdb)
                       superagent.put(cdb)
                       .type('json')
                       .auth(options.couchdb.auth.username
                            ,options.couchdb.auth.password)
                       .end(function(err,result){

                           if(result.error===undefined){
                               created_locally=true
                           }else{
                               console.log(result)
                           }
                           cb()
                       })
                   }
                  ,done
                  );
        return null
    })
})
after(function(done){
    if(created_locally){
        async.each([options.couchdb.db,options.couchdb.statedb]
                  ,function(db,cb){
                       var cdb =
                           [task.options.couchdb.url
                           ,db].join('/')
                       console.log(cdb)
                       superagent.del(cdb)
                       .type('json')
                       .auth(options.couchdb.auth.username
                            ,options.couchdb.auth.password)
                       .end(cb)
                   })
            return null
    }else{
        return done()
    }
})

describe('can mark as inprocess',function(){
    before(function(done){
        in_process(task,function(err,state){
               should.not.exist(err)
               should.exist(state)
               state.should.have.length(3)
               filter_grids(task,function(doit){
                   should.exist(doit)
                   doit.should.be.ok;
                   return done()
               })
           })
    })
    it('should mark accumulate with array'
      ,function(done){
           in_process(task,function(err,state){
               should.not.exist(err)
               should.exist(state)
               state.should.have.length(3)
               filter_grids(task,function(doit){
                   should.exist(doit)
                   doit.should.not.be.ok;
                   return done()
               })
           })
       })
})



describe('filter out done grids',function(){
    var options;
    before(config_okay('config.json',function(err,c){
               options ={'couchdb':c.couchdb}

               // dummy up a done grid and a not done grid in a test db

           }))

    it('should return false for a done grid'
      ,function(done){
           var task={'cell_id':'189_72'
                    ,'year':2009
                    ,'options':options}
           filter_grids(task
                   ,function(test_result){
                        should.exist(test_result)
                        test_result.should.be.ok;
                        done()
                    })
       })
    it('should return true for a not done grid'
      ,function(done){
           var task={'cell_id':'189_72'
                    ,'year':2009
                    ,'options':options}
           filter_grids(task
                   ,function(test_result){
                        should.exist(test_result)
                        test_result.should.not.be.ok;
                        done()
                    })
       })
})
