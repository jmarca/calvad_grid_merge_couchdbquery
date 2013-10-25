

var checker = require('couch_check_state')

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
    console.log(task)
    checker({'db':task.options.couchdb.statedb
            ,'doc':task.cell_id
            ,'year':task.year
            ,'state':['county','airbasin','airdistrict']}
           ,function(err,state){
                if(err) throw new Error(err)
                task.state=state
                if(state.length<3){
                    return cb(true)
                }
                return cb(false)
            })
    return null
}
module.exports=filter_out_done

// fixme test that this will properly grab files that need work,
// ignore files that don't
