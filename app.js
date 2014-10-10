var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )

var async    = require( 'async' )
var winston  = require('winston')

var Elasticsearch = require( 'winston-elasticsearch' )

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
	new Elasticsearch( { level : 'info' } ),
    ]
})

global.logger.info( "Started" , { dir : config.datadir })

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
			    for( var j = 0; file_count > j; j++){
				if( typeof( saved[files[j]] ) != "undefined" )
				    return callback( null )
				
				saved[files[j]] = true
				return files[j] 
			    }
			})
		    })
		})()
	    }
	async.series( read_funcs, function( new_files ) {
	    console.log( new_files )
	})
	//fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
    })

})

