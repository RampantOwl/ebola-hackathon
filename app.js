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

var config   = require( './config/config' )


global.logger = new (winston.Logger)({
    transports: [
	new (winston.transports.Console)( { 'timestamp' : true } ),
	new Elasticsearch( { level : 'info' } ),
    ]
})

global.logger.info( "Started" , { dir : config.datadir })

fs.readdir( path.resolve( __dirname , config.datadir ), function( err , dirname ) {

    var count     = Object.keys( dirname ).length
    var funcs =  new Array()
    
    for ( var n = 0; count > n; n++ ){
	(function(){
	    var i = n
	    var dir = dirname[i]
	    funcs.push( 
		function( callback ) { 
		    var d = dir
		    var p = path.resolve( __dirname , config.datadir , d )		    
		    return callback( null ,   p ) 
		} )
	})()
    }

    
    async.series( funcs , function( err , results ) {
	var results_count = Object.keys( results ).length
	for( var i = 0; results_count > i; i++ ){
	    var stats = fs.statSync( results[i] )
	    if( stats.isDirectory && results[i].lastIndexOf( DATA_DIR_NAME_FILTER ) == ( results[i].length -  DATA_DIR_NAME_FILTER.length ) )
		console.log( results[i] )
	}
	//fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
    })

})

