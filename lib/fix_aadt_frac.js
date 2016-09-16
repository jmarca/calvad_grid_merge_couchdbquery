/**
"use strict"
var getter = require('couchdb_get_views')
var putview = require('couchdb_put_view')

var myview = require('../couchdb/fix_aadt_view.json')


function get_docs(opts){
    return function(cb){
        // cycle.  call and return when fixed.
        getter(opts,function(e,resp){
            var putdocs = []
            // for each docs, rename aadt to aadt_frac
            var docsleft = resp.rows.length
            if(docsleft>0){
                resp.rows.forEach(function(row){
                    var doc = row.doc
                    doc.aadt_frac = doc.aadt
                    delete(doc.aadt)
                    putdocs.push(doc)
                })
                bulk_docs(opts,putdocs,function(e,r){
                    if(e) throw new Error(e)
                    return cb(null,docsleft)
                })
                return null
            }else{
                return cb(null,0)
            }
        })
        return null
    }
}

var q
var fix_some
function async_loop(e,resp){
    if(e) throw new Error(e)
    if(resp > 0){
        q = queue(1)
        q.defer(fix_some)
    }
    q.await(async_loop)
}

function fix_hpms_aadt(task,cb){
    // put the view I need

    var opts = Object.assign({}
                             ,task.options.couchdb
                            )
    if(opts.db === undefined) opts.db = opts.grid_merge_couchdbquery_hpms_db
    opts.doc = myview
    putview(opts,function(e,r){
        var docsleft = true
        // now get from that view
        delete(opts.doc)
        opts.view = myview._id
        opts.include_docs=true
        opts.limit=500
        fix_some = get_docs(opts)
        // get all the docs.  This might be stupid, so use limit
        while(docsleft){
            q = queue(1)
            q.defer(fix_some)
            q.await(async_loop)
        }
        return cb()
    })

    // go to couchdb, get fractions for year for this cell_id
    var key = [task.cell_id,task.year].join('_')
    var opts = _.assign({}
                       ,task.options.couchdb
                       ,{startkey:key
                        ,'endkey':key+'-13' // there is no month 13; gets entire year
                        ,'view':'_all_docs' // alldocs, not any extraneous view
                        ,'include_docs':true})
    if(opts.db === undefined) opts.db = opts.grid_merge_couchdbquery_hpms_db

    getter(opts
          ,function(err,docs){
               if(err) throw new Error(err)
               task.hpms_fractions = {}
               var hours = 0
               var sums = {} // accumulate summed value on the fly
               if(task.hpms_fractions_sums){
                   sums = task.hpms_fractions_sums
               }
               if(task.hpms_fractions_hours === undefined){
                   task.hpms_fractions_hours = 0
               }
               if(docs !== undefined
                && docs.rows !== undefined
                && docs.rows.length>0){
                   //console.log(docs.rows.length + ' rows from hpms_fractions query')
                   console.log('doc keys',Object.keys(docs.rows[0].doc))
                   _.each(docs.rows[0].doc.aadt_frac
                         ,function(v,k){
                              if(sums[k] === undefined){
                                  sums[k]=+0
                              }
                          });

                   _.each(docs.rows,function(row){
                       task.hpms_fractions[row.doc.ts]={}
                       hours++
                       _.each(row.doc.aadt_frac,function(v,k){
                           if(!v) v = 0 // force null, undefined to be zero
                           task.hpms_fractions[row.doc.ts][k]=+v
                           sums[k]+=+v
                       })
                   });
               }else{
                   //console.log('hpms_fractions nothing')
                   if(docs.error !== undefined){
                       throw new Error(docs)
                   }
               }
               task.hpms_fractions_sums=sums
               task.hpms_fractions_hours = task.hpms_fractions_hours > hours ?
                   task.hpms_fractions_hours : hours
               return cb(null,task)
           })
    return null
}
*/
