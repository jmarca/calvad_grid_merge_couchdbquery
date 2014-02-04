
var _ = require('lodash')

var f_system = require('./f_system.json')
// add an entry for detector_based "class" of highway
f_system.detector_based='detector based hwy data'

/**
 * flatten out the sums into a giant data matrix, and save it
 *
 */

function flatten_records(task,cb){
    // make something like
    // area_type: county/basin/district
    // area_name: ..
    // year: ..
    // header: variable names
    // data:  data rows
    // grid_cells: [grid cells used]


    var doc = {'area_type':task.area_type
              ,'area_name':task.area_name
              ,'year':task.year
              ,'header':['ts'
                        ,'road_class'
                        ,'vmt'
                        ,'lane_miles'
                        ,'single_unit_mt'
                        ,'combination_mt'
                        ]
              ,'data':[]
              ,'grid_cells':task.grid_cells
              }
    // assign an id here
    doc._id = [doc.area_type,doc.area_name,doc.year].join('_')

    _.each(task.result,function(roads,ts){
        _.each(roads,function(record,road_class){
            // fill in the data array
            var d = [ts,f_system[road_class]]
            // either hpms or detector record
            if(record.n_mt !== undefined){
                // working on a detector-based record
                d.push([record.n_mt
                       ,record.lane_miles
                       ,record.nhh_mt
                       ,record.hh_mt])
            }else{
                // working on a detector-based record
                d.push([record.sum_vmt
                       ,record.sum_lane_miles
                       ,record.sum_single_unit_mt
                       ,record.sum_combination_mt])

            }

            doc.data.push(_.flatten(d))

            return null
        });
        return null
    });
    return cb(null,doc)
}

exports.flatten_records=flatten_records
