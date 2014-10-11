var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )

var async    = require( 'async' )
var winston  = require('winston')

var client = require('simple-elasticsearch').client.create();

var DATA_SUBSTR_OFFSET = 5
var DATA_DIR_NAME_FILTER = "_data"

var SAVE_FILE = "./config/saved"

var saved = JSON.parse ( fs.readFileSync( SAVE_FILE ) )
if ( typeof( saved.files) == "undefined" ) 
    saved.files = { }

var config   = require( './config/config' )


global.logger = new (winston.Logger)({
    transports: [
	new (winston.transports.Console)( { 'timestamp' : true } ),
    ]
})

global.logger.info( "Started" , { dir : config.datadir })

function getObject( file ){
    var data = fs.readFileSync( file ).toString().split('\n')
    var country = file.match(/([a-z]*)_data/g)[0].replace("_data","")

    var lines = data.length

    var keys = data[0].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'').split(',')
    for( var i=1; lines > i; i++){
	var line_data = data[i].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'').split(',')
	var stat = line_data[1]
	var datetime = new Date( line_data[0] )
	
	if( typeof( stat ) == "undefined" )
	    continue
	
	var key_length = Object.keys( keys ).length
	var obj = {}
	for( var k =2; key_length > k; k++ ){
	    if( typeof( line_data[k] ) == "undefined" ) 
		continue
	    
	    var doc = { }
	    doc['region'] = keys[k]
	    doc['value'] = line_data[k]	    
	    doc['@timestamp'] = datetime
	    
	    //doc[keys[k]] = line_data[k]
	    client.core.index( { index: country , type: line_data[1], '@timestamp' : datetime  ,  doc : doc  } , function( err , result ){ 
	    } )
	}
    }    
}
fs.readdir( path.resolve( __dirname , config.datadir ), function( err , files ) {

    var count     = Object.keys( files ).length
    var funcs =  new Array()
    
    for ( var n = 0; count > n; n++ ){
	(function(){
	    var i = n
	    var dir = files[i]
	    funcs.push( 
		function( callback ) { 
		    var resolved_path = path.resolve( __dirname , config.datadir , files[i] )  
		    return callback( null ,  resolved_path  ) 
		} )
	})()
    }

    
    async.series( funcs , function( err , results ) {
	var read_funcs = new Array()
	var results_count = Object.keys( results ).length
	for( var i = 0; results_count > i; i++ ){
	    var resolved_path = results[i]
	    var stats = fs.statSync( resolved_path )
	    if( stats.isDirectory && resolved_path.lastIndexOf( DATA_DIR_NAME_FILTER ) == ( resolved_path.length -  DATA_DIR_NAME_FILTER.length ) )
		(function(){
		    var r_path =resolved_path
		    read_funcs.push( function( callback ) {
			fs.readdir( r_path , function( err , files ) {
			    var file_count = Object.keys( files ).length
			    var file_list  = new Array()
			    for( var j = 0; file_count > j; j++){
				/* Filter out data files*/
				if( (files[j].indexOf('.csv') > -1 ) && (files[j].indexOf('ebola_') > -1 || files[j].indexOf('case_') > -1 ) ){
				    if( typeof( saved[files[j]] ) == "undefined" ){
					saved[files[j]] = true
					file_list.push( path.resolve( r_path , files[j] ) )
				    }
				}
			    }
			    return callback( null , file_list )
			})
		    })
		})()
	}
	async.series( read_funcs, function( err , data_files ) {
	    
	    for( var i=0; Object.keys( data_files ).length > i; i++)
		for( var j=0; Object.keys( data_files[i] ).length > j; j++){
		    var object = getObject( data_files[i][j] )
		    console.log( data_files[i][j] )

		}

	    //fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
	})
	//fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
    })
    
})

