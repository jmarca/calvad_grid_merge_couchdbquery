/* global require console process it describe after before */

var should = require('should')

var async = require('async')
var _ = require('lodash')
var filter_grids = require('../lib/filter_grids')
var get_detector_routes = require('../lib/query_postgres').get_detector_route_nums
var fs = require('fs')



describe('get_hpms_from_sql',function(){
    var config,options
    before(function(){
        fs.stat('config.json',function(err,stats){
            should.not.exist(err)
            should.exist(stats)
            console.log( stats.mode.toString(8) )
            stats.mode.toString(8).should.eql('100600')
            config = require('../config.json')
            options ={'couchdb':config.couchdb}

        })
    })
    it('should get data from sql'
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
})
