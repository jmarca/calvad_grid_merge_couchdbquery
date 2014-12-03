/*global module require */
var grid_records= require('calvad_areas').grid_records
var queue = require("queue-async")
var _ = require('lodash')

// map reduce.  this is the map part.  take the list of grids, and
// then extract the ones that are not yet done, and make task objects
// for each, and then return

var config_okay = require('config_okay')

var filter_grids = require('./couchdb_interactions').filter_out_done

function load_config(config_file){
    return function(cb){
        config_okay(config_file,function(err,config){
            return cb(null,{'couchdb':config.couchdb})
        })
        return null
    }
}

function all_tasks(year){
    return function(config,cb_alltasks){
        var tasks = _.map(grid_records
                         ,function(v,k){
                              var task = {'options':_.clone(config)}
                              task.cell_id = k
                              task.year = year
                              _.extend(task,v)
                              return task
                          })
        function checkit(task,callback){
            filter_grids(task,function(err,result){
                if(result){
                    filtered_tasks.push(task)
                }
                return callback(err,result)
            })
            return null
        }
        var q = queue(4)
        tasks.forEach(function(task){
            q.defer(filter_grids,task)
        })
        q.awaitAll(function(err,tasks){
            if(err) return cb_alltasks(err)
            var filtered_tasks = tasks.filter(function(task){
                                 return task.todo
                             })
            return cb_alltasks(null,filtered_tasks)
        })
        return null
    }
}

/**
 * make_map(year,config_file,callback)
 *
 * arguments:
 *
 * year: the year of data
 * config_file: the name of the config file to load
 * callback:  will be called with error as first arg, a list of tasks as second
 *            if no error
*/
var make_map = function(year,config_file,cb){

    async.waterfall([load_config(config_file)
                    ,all_tasks(year)]
                   ,cb)
    return null
}

module.exports=make_map