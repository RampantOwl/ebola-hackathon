var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )

var async    = require( 'async' )
var winston  = require('winston')

var Elasticsearch = require( 'winston-elasticsearch' )

var config   = require( './config/config' )

var DATA_SUBSTR_OFFSET = 5
var DATA_DIR_NAME_FILTER = "_data"
global.logger = new (winston.Logger)({
    transports: [
	new (winston.transports.Console)( { 'timestamp' : true } ),
	new Elasticsearch( { level : 'info',
			     port : 9200 ,
			     host : 'localhost' 
			   } )
    ]
})

global.logger.info( "Started" , { dir : config.datadir })

fs.readdir( path.resolve( __dirname , config.datadir ), function( err , dirname ) {
    var count = Object.keys( dirname ).length

    for ( var n = 0; count > n; n++ )
	if( dirname[n].substring( dirname[n].length - DATA_SUBSTR_OFFSET )  == DATA_DIR_NAME_FILTER )
	    global.logger.info( "Match Directory" ,  { dir : dirname[n] } )
})

