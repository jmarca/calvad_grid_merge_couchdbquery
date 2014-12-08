/* global require console process it describe after before */

var should = require('should')

var queue = require('queue-async')
var _ = require('lodash')

var cdb_interactions = require('../lib/couchdb_interactions')
var filter_grids = cdb_interactions.filter_out_done
var mark_done = cdb_interactions.mark_done
var in_process = cdb_interactions.mark_in_process
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var get_detector_fractions = cdb_interactions.get_detector_fractions

var fs = require('fs')
var superagent=require('superagent')

var config_okay = require('config_okay')

var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()

var task

var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'
var options
before(function(done){
    config_okay(config_file,function(err,c){
        options ={'couchdb':c.couchdb}
        options.couchdb.hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_db += test_db_unique
        options.couchdb.state_db += test_db_unique

        // dummy up a done grid and a not done grid in a test db
        task = {'options':options};
        utils.demo_db_before(options)(done)
        return null
    })
    return null
})
after(function(done){
    utils.demo_db_after(options)(done)
    return null
})


describe('get hpms fractions',function(){

    it('can get data for a known grid',function(done){
        task.cell_id='100_223'
        task.year=2008
        get_hpms_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            task.should.have.property('hpms_fractions')
            _.keys(task.hpms_fractions).length.should.eql(utils.hpms_docs)
            task.should.have.property('hpms_fractions_sums')
            task.should.have.property('hpms_fractions_hours')
            _.each(task.hpms_fractions,function(record){
                record.should.have.keys('n','hh','nhh')
            })
            return done()
        })
    })

    it('will not crash if an unkown grid is passed in',function(done){
        task.cell_id='101_223'
        task.year=2008
        get_hpms_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            return done()
        })
    })

    it('will merge multiple grid cells by time',function(done){
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
        task.cell_id='101_223'
        task.year=2008
        get_detector_fractions(task,function(err,task){
            should.not.exist(err)
            should.exist(task)
            return done()
        })
    })

    it('will merge multiple grid cells by time',function(done){
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
                       });
                task.should.have.property('detector_fractions_hours',hours)
                task.should.have.property('detector_data')
                _.size(task.detector_data).should.be.eql(utils.detector_docs)
                _.each(task.detector_data,function(record,ts){
                    record.should.be.instanceOf(Array)
                    record.length.should.be.above(16)
                })
                return done()
            })
        })
    })

})
