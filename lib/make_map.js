/*global module require */
var grid_records= require('calvad_areas').grid_records
var async = require('async')
var _ = require('lodash')
// map reduce.  this is the map part.  take the list of grids, and
// then extract the ones that are not yet done, and make task objects
// for each, and then return

var config_okay = require('../lib/config_okay')

var filter_grids = require('../lib/couchdb_interactions').filter_out_done

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
        var filtered_tasks = []
        var q = async.queue(function(task,callback){
                    filter_grids(task,function(doit){
                        if(doit){
                            filtered_tasks.push(task)
                        }
                        return callback()
                    })
                },100) // trialled also with 10 and 1000. 100 wins
        // assign a callback for when the queue drains
        q.drain = function() {
            console.log('all items have been processed');
            cb_alltasks(null,filtered_tasks)
        }
        q.push(tasks)
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