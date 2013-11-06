/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var cdb_interactions = require('../lib/couchdb_interactions')
var filter_grids = cdb_interactions.filter_out_done
var mark_done = cdb_interactions.mark_done
var in_process = cdb_interactions.mark_in_process
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var get_detector_fractions = cdb_interactions.get_detector_fractions

var get_detector_routes = require('../lib/query_postgres').get_detector_route_nums
var fs = require('fs')
var superagent=require('superagent')

var config_okay = require('../lib/config_okay')

var date = new Date()
var test_db_unique = date.getHours()+'-'+date.getMinutes()+'-'+date.getSeconds()+'-'+date.getMilliseconds()

var task

var utils = require('./utils')

before(function(done){
    config_okay('test.config.json',function(err,c){
        var options ={'couchdb':c.couchdb}
        options.couchdb.db += test_db_unique
        options.couchdb.statedb += test_db_unique

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options};
        var datadb = options.couchdb.db
        async.each([options.couchdb.db,options.couchdb.statedb]
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


describe('get hpms fractions',function(){

    it('can get data for a known grid',function(done){
        task.options.couchdb.hpms_fractions_db=task.options.couchdb.db
        task.cell_id='100_223'
        task.year=2008
        get_hpms_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            task.should.have.property('hpms_fractions')
            _.keys(task.hpms_fractions).length.should.eql(utils.hpms_docs)
            task.should.have.property('hpms_fractions_sums')
            task.should.have.property('hpms_fractions_hours')
            return done()
        })
    })

    it('will not crash if an unkown grid is passed in',function(done){
        task.options.couchdb.hpms_fractions_db=task.options.couchdb.db
        task.cell_id='101_223'
        task.year=2008
        get_hpms_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            return done()
        })
    })

    it('will merge multiple grid cells by time',function(done){
        task.options.couchdb.hpms_fractions_db=task.options.couchdb.db
        task.cell_id='100_223'
        task.year=2008
        get_hpms_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            task.should.have.property('hpms_fractions_sums')
            task.cell_id='178_97'
            var sums=_.clone(task.hpms_fractions_sums)
            var hours=_.clone(task.hpms_fractions_hours)
            get_hpms_fractions(task,function(err,task){
                should.not.exist(err)
                should.exist(task)
                task.should.have.property('hpms_fractions')
                _.keys(task.hpms_fractions).length.should.eql(utils.hpms_docs)
                task.should.have.property('hpms_fractions_sums')
                _.each(task.hpms_fractions_sums
                      ,function(v,k){
                           v.should.be.above(sums[k])
                           return null
                       })
                    task.should.have.property('hpms_fractions_hours',hours)
                return done()
            })
        })
    })

})

describe('get detector fractions',function(){

    it('can get data for a known grid',function(done){
        task.options.couchdb.detector_data_db=task.options.couchdb.db
        task.cell_id='189_72'
        task.year=2008
        get_detector_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            task.should.have.property('detector_fractions')
            _.keys(task.detector_fractions).should.have.lengthOf(utils.detector_docs)
            task.should.have.property('detector_fractions_sums')
            task.should.have.property('detector_fractions_hours',utils.detector_docs)
            //console.log(task)
            return done()
        })
    })

    it('will not crash if an unkown grid is passed in',function(done){
        task.options.couchdb.detector_fractions_db=task.options.couchdb.db
        task.cell_id='101_223'
        task.year=2008
        get_detector_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            return done()
        })
    })

    it('will merge multiple grid cells by time',function(done){
        task.options.couchdb.detector_fractions_db=task.options.couchdb.db
        task.cell_id='189_72'
        task.year=2008
        get_detector_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            task.should.have.property('detector_fractions_sums')
            // just repeat the same grid cell for the test
            var sums=_.clone(task.detector_fractions_sums)
            var hours=_.clone(task.detector_fractions_hours)
            get_detector_fractions(task,function(err,task){
                should.not.exist(err)
                should.exist(task)
                task.should.have.property('detector_fractions')
                task.should.have.property('detector_fractions_sums')
                _.each(task.detector_fractions_sums
                      ,function(v,k){
                           v.should.be.above(sums[k])
                           return null
                       })
                    task.should.have.property('detector_fractions_hours',hours)
                return done()
            })
        })
    })

})
