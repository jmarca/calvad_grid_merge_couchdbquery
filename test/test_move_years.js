/*global require before describe after it console */

// test for move_to_year_cdb.js

var cdb_interactions = require('../lib/couchdb_interactions')
var get_hpms_fractions = cdb_interactions.get_hpms_fractions
var reduce = require('../lib/reduce')
var get_detector_fractions = cdb_interactions.get_detector_fractions

var mty = require('../lib/move_to_year_cdb.js')

var config_okay = require('config_okay')

var should = require('should')

var queue = require('d3-queue').queue
var _ = require('lodash')
var utils = require('./utils')
var path = require('path')
var rootdir = path.normalize(__dirname)
var config_file = rootdir+'/../test.config.json'

var superagent = require('superagent')
var date = new Date()
var test_db_unique = date.getHours()+'-'
                   + date.getMinutes()+'-'
                   + date.getSeconds()+'-'
                   + date.getMilliseconds()+'-'+Math.floor(Math.random() * 1000)

var options = {}

before(function(done){
    config_okay(config_file,function(err,c){
        options.couchdb= _.extend({},c.couchdb)
        options.couchdb.grid_merge_couchdbquery_hpms_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_detector_db += test_db_unique
        options.couchdb.grid_merge_couchdbquery_state_db += test_db_unique

        utils.demo_db_before(options)(function(e,r){
            var nameddbs = [options.couchdb.grid_merge_couchdbquery_detector_db
                            ,options.couchdb.grid_merge_couchdbquery_hpms_db
                            ,options.couchdb.grid_merge_couchdbquery_state_db
                           ]
            var q = queue(5)
            nameddbs.map(function(db){
                q.defer(utils.create_tempdb,{'options':options},db+'%2f2008')
                q.defer(utils.create_tempdb,{'options':options},db+'%2f2009')
                return  null
            })
            q.await(function(e){
                queue()
                    .defer(utils.put_view,
                           './lib/couchdb_view.json',
                           _.assign({},options.couchdb,{'db':nameddbs[0]+'%2f2008'}))
                    .defer(utils.put_view,
                           './lib/couchdb_view.json',
                           _.assign({},options.couchdb,{'db':nameddbs[0]+'%2f2009'}))
                    .defer(utils.put_view,
                           './lib/couchdb_hpms_view.json',
                           _.assign({},options.couchdb,{'db':nameddbs[1]+'%2f2008'}))
                    .defer(utils.put_view,
                           './lib/couchdb_hpms_view.json',
                           _.assign({},options.couchdb,{'db':nameddbs[1]+'%2f2009'}))
                    .await(done)
                return null
            })

            return null
        })
        return null
    })
    return null
})

after(function(done){
    var nameddbs = [options.couchdb.grid_merge_couchdbquery_detector_db
                    ,options.couchdb.grid_merge_couchdbquery_hpms_db
                    ,options.couchdb.grid_merge_couchdbquery_state_db
                   ]
    var q = queue(5)
    nameddbs.map(function(db){
        q.defer(utils.delete_tempdb,{'options':options},db)
        q.defer(utils.delete_tempdb,{'options':options},db+'%2f2008')
        q.defer(utils.delete_tempdb,{'options':options},db+'%2f2009')
        q.defer(utils.delete_tempdb,{'options':options},db+'%2f2012')
        return  null
    })
    q.await(function(e){
        return done()
    })
    return null
})

describe('post_process_hpms_couch_query',function(){

    it('should fail on a 2008 hpms result',function(done){
        // before moving years, the 2008 call should fail because the
        // 2008 cdb exists but is empty
        var task ={'options':options
                  ,'cell_id':'100_223'
                  ,'year':2008
                  }
        task.should.not.have.property('scale')
        queue(1)
            .defer(get_hpms_fractions,task)
            .defer(reduce.post_process_couch_query,task)
            .await(function(e,r){
                //console.log(r)
                should.not.exist(e)
                should.exist(task)
                task.should.have.property('scale')
                task.scale.n.should.eql(1)
                //.be.approximately(1.0164381992, 0.00001)
                task.scale.hh.should.eql(1)
                //.be.approximately(0.9912986681, 0.00001)
                task.scale.nhh.should.eql(1)
                //.be.approximately(1.0068673223, 0.00001)
                task.fractions.should.eql({})
                return done()
            });

    })
    it('should correctly post process a 2012 result',function(done){
        var task ={'options':options
                  ,'cell_id':'128_172'
                  ,'year':2012
                  }
        task.should.not.have.property('scale')
        queue(1)
        .defer(get_detector_fractions,task)
        .defer(reduce.post_process_couch_query,task)
        .await(function(e){
                         should.not.exist(e)
                         should.exist(task)
                         task.should.have.property('scale')
                         task.scale.n.should.be
                         .approximately(1.0, 0.00001)
                         task.scale.hh.should.be
                         .approximately(1.0, 0.00001)
                         task.scale.nhh.should.be
                         .approximately(1.0, 0.00001)
                         return done()
                     });

    })
    it('should successfully migrate 2008, 2009 data',function(done){
        mty(options,function(e,r){
            // after moving years, the 2008 call should work now
            // because the 2008 cdb exists AND has been populated
        var task ={'options':options
                  ,'cell_id':'100_223'
                  ,'year':2008
                  }
        //console.log(task)
            task.should.not.have.property('scale')
            queue(1)
                .defer(get_hpms_fractions,task)
                .defer(reduce.post_process_couch_query,task)
                .await(function(e){
                    should.not.exist(e)
                    should.exist(task)
                    task.should.have.property('scale')
                    task.scale.n.should.be
                        .approximately(1.0164381992, 0.00001)
                    task.scale.hh.should.be
                        .approximately(0.9912986681, 0.00001)
                    task.scale.nhh.should.be
                        .approximately(1.0068673223, 0.00001)
                    task.fractions.should.have.property('100_223')
                    console.log(task.fractions)
                    done()
                });

    })
})
