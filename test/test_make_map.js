/* global require console process it describe after before */

var should = require('should')

var make_map = require('../lib/make_map')
var grid_records= require('calvad_areas').grid_records


var config_okay = require('config_okay')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var config={}
before(function(done){
    config_okay(config_file,function(err,c){
        should.not.exist(err)
        config.couchdb =c.couchdb
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

describe('make a map',function(){
    it('should make a map with all the grids',function(done){
        make_map(2007,config,function(e,t){
            should.not.exist(e)
            should.exist(t)
            t.should.be.an.instanceOf(Array)
            t.should.have.lengthOf(Object.keys(grid_records).length)
            return done()
        })
    })
})
