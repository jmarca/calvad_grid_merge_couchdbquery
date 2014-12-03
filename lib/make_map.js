/*global module require */
var grid_records= require('calvad_areas').grid_records
var queue = require("queue-async")
var _ = require('lodash')

// map reduce.  this is the map part.  take the list of grids, and
// then extract the ones that are not yet done, and make task objects
// for each, and then return

var filter_grids = require('./couchdb_interactions').filter_out_done

var ij_regex = /(\d*)_(\d*)/;
function all_tasks(year,config,cb){
    var tasks = _.map(grid_records
                     ,function(v,cell_id){
                          var re_result = ij_regex.exec(cell_id)
                          var task = {'options':_.clone(config)}
                          task.cell_id = cell_id
                          task.cell_i = re_result[1]
                          task.cell_j = re_result[2]
                          task.year = year
                          _.extend(task,v)
                          return task
                      })
    var q = queue(4)
    tasks.forEach(function(task){
        q.defer(filter_grids,task)
    })
    q.awaitAll(function(err,tasks){
        if(err) return cb(err)
        var filtered_tasks = tasks.filter(function(task){
                                 return task.todo
                             })
        return cb(null,filtered_tasks)
    })
    return null
}


module.exports=all_tasks