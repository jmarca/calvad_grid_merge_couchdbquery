/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')

var make_map = require('../lib/make_map')
var grid_records= require('calvad_areas').grid_records

describe('make a map',function(){
    it('should make a map with all the grids',function(done){
        make_map(2007,'./config.json',function(e,t){
            should.not.exist(e)
            should.exist(t)
            t.should.be.an.instanceOf(Array)
            t.should.have.lengthOf(Object.keys(grid_records).length)
            return done()
        })
    })
})