/*global require exports */
var superagent = require('superagent')

var queue=require('queue-async')

var _ = require('lodash')
var should = require('should')
var fs = require('fs')

function create_tempdb(task,db,cb){
    if(typeof db === 'function'){
        throw new Error('db required now')
    }
    var cdb =
        [task.options.couchdb.host+':'+task.options.couchdb.port
        ,db].join('/')
    superagent.put(cdb)
    .type('json')
    .auth(task.options.couchdb.auth.username
         ,task.options.couchdb.auth.password)
    .end(function(err,result){
        cb()
    })
    return null
}

function delete_tempdb(task,db,cb){
    if(typeof db === 'function'){
        throw new Error('db required now')
    }
    var cdb =
        [task.options.couchdb.host+':'+task.options.couchdb.port
        ,db].join('/')
    superagent.del(cdb)
    .type('json')
    .auth(task.options.couchdb.auth.username
         ,task.options.couchdb.auth.password)
    .end(cb)
    return null
}


var hpms_docs=0
var detector_docs=0
var viewput = require('couchdb_put_view')

function post_file(file,couch,doclen,cb){

    var db_dump = require(file)
    var docs = _.map(db_dump.rows
                    ,function(row){
                         return row.doc
                     })
    doclen += docs.length

    superagent.post(couch+'/_bulk_docs')
    .type('json')
    .send({"docs":docs})
    .end(function(e,r){
        should.not.exist(e)
        should.exist(r)
        return cb(null,doclen)
    })
    return null
}


function put_view(viewfile,config,cb){
    fs.readFile(viewfile, function (err, data) {
        var design_doc;
        var opts;
        if (err) throw err;
        opts = _.assign({},config,{'doc':JSON.parse(data)})
        viewput(opts,cb)
        return null
    })
    return null
}


function put_file(file,couch,cb){

    var db_dump = require(file)
    superagent.post(couch)
    .type('json')
    .send(db_dump)
    .end(function(e,r){
        should.not.exist(e)
        should.exist(r)
        return cb(e)
    })
    return null
}

function load_hpms(task,cb){
    var db_files = ['./files/100_223_2008_JAN.json'
                   ,'./files/178_97_2008_JAN.json'
                   ,'./files/134_163_2008_JAN.json']
    var cdb = [task.options.couchdb.host+':'+task.options.couchdb.port
              ,task.options.couchdb.grid_merge_couchdbquery_hpms_db].join('/')

    var q = queue()
    db_files.forEach(function(file){
        q.defer(post_file,file,cdb,0)
    })
    // the 2012 data goes into a 2012 year specific db for testing the year stuff
    q.defer(post_file,'./files/100_223_2012_JAN_hpms.json',cdb + '%2f2012',0)

    q.await(function(err,d1,d2,d3,d4){
        should.not.exist(err)
        superagent.get(cdb)
        .type('json')
        .end(function(e,r){
            should.not.exist(e)
            should.exist(r)
            r.should.have.property('text')
            var superagent_sucks = JSON.parse(r.text)
            superagent_sucks.should.have.property('doc_count',d1+d2+d3)
            return cb()

        })
        return null
    })
    return null
}

function load_detector(task,cb){
    var db_files = ['./files/132_164_2008_JAN.json'
                   ,'./files/132_164_2009_JAN.json'
                   ,'./files/189_72_2008_JAN.json'
                   ,'./files/134_163_2008_JAN_detector.json']
    var cdb = [task.options.couchdb.host+':'+task.options.couchdb.port
              ,task.options.couchdb.grid_merge_couchdbquery_detector_db].join('/')
    var q = queue()
    db_files.forEach(function(file){
        q.defer(post_file,file,cdb,0)
    })
    // the 2012 data goes into a 2012 year specific db for testing the year stuff
    q.defer(post_file,'./files/128_172_2012_JAN_detectors.json',cdb + '%2f2012',0)
    q.await(function(err,d1,d2,d3,d4,d5){
        should.not.exist(err)
        superagent.get(cdb)
        .type('json')
        .end(function(e,r){
            should.not.exist(e)
            should.exist(r)
            r.should.have.property('text')
            var superagent_sucks = JSON.parse(r.text)
            superagent_sucks.should.have.property('doc_count',d1+d2+d3+d4)
            return cb()
        })
        return null
    })
    return null
}


function load_area_sums(task,cb){
    var db_files = ['./files/NC.json']
    var cdb = [task.options.couchdb.host+':'+task.options.couchdb.port
              ,task.options.couchdb.grid_merge_couchdbquery_put_db].join('/')
    var q = queue()
    db_files.forEach(function(file){
        q.defer(put_file,file,cdb)
    })
    q.awaitAll(function(err,res){
        should.not.exist(err)
        superagent.get(cdb)
        .type('json')
        .end(function(e,r){
            should.not.exist(e)
            should.exist(r)
            r.should.have.property('text')
            var superagent_sucks = JSON.parse(r.text)
            superagent_sucks.should.have.property('doc_count',1)
            return cb()
        })
        return null
    })
    return null
}



function demo_db_before(config){
    return function(done){
        var task = {options:config}
        // dummy up a done grid and a not done grid in a test db
        var dbs = [task.options.couchdb.grid_merge_couchdbquery_detector_db
                  ,task.options.couchdb.grid_merge_couchdbquery_hpms_db
                  ,task.options.couchdb.grid_merge_couchdbquery_state_db
                  ]

        var q = queue(1)
        // also make a 2012 db for testing the year thing
        dbs.forEach(function(db){
            if(!db) return null
            q.defer(create_tempdb,task,db)
            q.defer(create_tempdb,task,db+'%2f2012')
            return null
        })
        q.await(function(e){
            should.not.exist(e)
            queue(1)
                .defer(load_hpms,task)
                .defer(load_detector,task)
                .defer(put_view,
                       './lib/couchdb_view.json',
                       _.assign({},config.couchdb,{'db':dbs[0]}))
                .defer(put_view,
                       './lib/couchdb_hpms_view.json',
                       _.assign({},config.couchdb,{'db':dbs[1]}))
                .defer(put_view,
                       './lib/couchdb_view.json',
                       _.assign({},config.couchdb,{'db':dbs[0]+'%2f2012'}))
                .defer(put_view,
                       './lib/couchdb_hpms_view.json',
                       _.assign({},config.couchdb,{'db':dbs[1]+'%2f2012'}))
            .await(done)
            return null
        })
        return null
    }
}

function demo_db_after(config){
    return  function(done){
        var task = {options:config}
        var dbs = [task.options.couchdb.grid_merge_couchdbquery_detector_db
                  ,task.options.couchdb.grid_merge_couchdbquery_hpms_db
                  ,task.options.couchdb.grid_merge_couchdbquery_state_db
                  ]


        var q = queue()
        dbs.forEach(function(db){
            if(!db) return null
            q.defer(delete_tempdb,task,db)
            // also the 2012 dbs
            q.defer(delete_tempdb,task,db+'%2f2012')
            return null
        })
        q.await(done)
        return null

    }
}

exports.load_area_sums = load_area_sums
exports.load_detector = load_detector
exports.load_hpms     = load_hpms
exports.create_tempdb = create_tempdb
exports.delete_tempdb = delete_tempdb
exports.hpms_docs     = 744 // 24 hours, 31 days
exports.detector_docs = 744
exports.demo_db_after = demo_db_after
exports.demo_db_before= demo_db_before
