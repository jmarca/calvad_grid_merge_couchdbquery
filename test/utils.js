/*global require exports */
var superagent = require('superagent')
var async = require('async')
var _ = require('lodash')
var should = require('should')

function create_tempdb(task,cb){
    var cdb =
        [task.options.couchdb.url+':'+task.options.couchdb.port
        ,task.options.couchdb.db].join('/')
    superagent.put(cdb)
    .type('json')
    .auth(task.options.couchdb.auth.username
         ,task.options.couchdb.auth.password)
    .end(function(err,result){
        cb()
    })
}

var hpms_docs=0
var detector_docs=0

function load_hpms(task,cb){
    async.each(['./files/100_223_2008_JAN.json'
               ,'./files/178_97_2008_JAN.json']
              ,function(file,innercb){

                   var db_dump = require(file)
                   var docs = _.map(db_dump.rows
                                   ,function(row){
                                        return row.doc
                                    })
                   var cdb = [task.options.couchdb.url+':'+task.options.couchdb.port
                             ,task.options.couchdb.hpms_db].join('/')
                   var couch =  cdb
                   superagent.post(couch+'/_bulk_docs')
                   .type('json')
                   .send({"docs":docs})
                   .end(function(e,r){
                       hpms_docs += docs.length
                       superagent.get(couch)
                       .type('json')
                       .end(function(e,r){
                           should.exist(r)
                           r.should.have.property('text')
                           var superagent_sucks = JSON.parse(r.text)
                           superagent_sucks.should.have
                           .property('doc_count',hpms_docs)
                           return innercb()
                       })
                       return null
                   })
               }
              ,function(e){
                   return cb(e)
               })
        return null
}

function load_detector(task,cb){
    var db_dump = require('./files/189_72_2008_JAN.json')
    var docs = _.map(db_dump.rows
                    ,function(row){
                         return row.doc
                     })
    detector_docs = docs.length
    var cdb = [task.options.couchdb.url+':'+task.options.couchdb.port
              ,task.options.couchdb.detector_db].join('/')
    var couch = cdb
    superagent.post(couch+'/_bulk_docs')
    .type('json')
    .send({"docs":docs})
    .end(function(e,r){
        superagent.get(couch)
        .type('json')
        .end(function(e,r){
            should.exist(r)
            r.should.have.property('text')
            var superagent_sucks = JSON.parse(r.text)
            superagent_sucks.should.have.property('doc_count',detector_docs)
            return cb()
        })
        return null
    })
}


function demo_db_before(config){
    return function(done){
        var task = {options:config}
        // dummy up a done grid and a not done grid in a test db

        async.each([task.options.couchdb.detector_db
                   ,task.options.couchdb.hpms_db
                   ,task.options.couchdb.state_db]
                  ,function(db,cb){
                       task.options.couchdb.db=db
                       create_tempdb(task,cb)
                       return null
                   }
                  ,function(){
                       async.series([function(cb){
                                           load_hpms(task,cb)
                                           return null
                                       }
                                      ,function(cb){
                                           load_detector(task,cb)
                                           return null
                                       }]
                                     ,done)
                   }
                  );
        return null
    }

}
function demo_db_after(config){
    return  function(done){
        var task = {options:config}
        async.each([task.options.couchdb.detector_db
                   ,task.options.couchdb.hpms_db
                   ,task.options.couchdb.state_db]
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

    }
}


exports.load_detector = load_detector
exports.load_hpms     = load_hpms
exports.create_tempdb = create_tempdb
exports.hpms_docs     = 744 // 24 hours, 31 days
exports.detector_docs = 744
exports.demo_db_after = demo_db_after
exports.demo_db_before= demo_db_before
