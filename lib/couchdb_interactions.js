var setter = require('couch_set_state')
var checker = require('couch_check_state')
var getter = require('couchdb_get_views')

/**
 * filter_out_done
 *
 * arguments:
 *
 * task: an object, with a cell_id member and a year
 * cb: a callback, will get true for files that are NOT done,
 *     or false for files that are done
 *
 * The false for done files will filter them out of the resulting list
 *
 * Use this in async.filter, as in
 * async.filter(jobs,filter_done,function(todo_jobs){
 *   // todo jobs has only the task items that have not yet been done
 * })
 *
 */
var filter_out_done = function(task,cb){
    // go look up in couchdb
    checker({'db':task.options.couchdb.statedb
            ,'couchdb':task.options.couchdb.url
            ,'port':task.options.couchdb.port
            ,'doc':task.cell_id
            ,'year':task.year
            ,'state':'accumulate'}
           ,function(err,state){
                if(err) throw new Error(err)
                if(state){
                    task.state=state
                }
                if(! state || (state && state.length<3)){
                    return cb(true)
                }
                return cb(false)
            })
    return null
}


// mark the data as being "in process"
var in_process = function(task,cb){
    setter({'db':task.options.couchdb.statedb
           ,'couchdb':task.options.couchdb.url
           ,'port':task.options.couchdb.port
           ,'doc':task.cell_id
           ,'year':task.year
           ,'state':'accumulate'
           ,'value':['_county','_airbasin','_airdistrict']
           }
          ,function(err){
               if(err){
                   console.log(err)
                   throw new Error(err)
               }
               cb(null)
           }
          )
    return null
}



var done = function(task,cb){
    setter({'db':[task.options.couchdb.url,task.options.couchdb.statedb].join('/')
           ,'doc':task.cell_id
           ,'year':task.year
           ,'state':'accumulate'
           ,'value':task.finished_areas
           }
          ,function(err,state){
               if(err) throw new Error(err)
               cb(null,state)
           }
          )
    return null
}



exports.filter_out_done=filter_out_done
exports.mark_done=done
exports.mark_in_process=in_process
