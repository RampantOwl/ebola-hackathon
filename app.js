var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )
var util     = require( 'util' )
var async    = require( 'async' )

var winston  = require('winston')
var parse    = require( 'csv-parse' )
var CSV = require( 'csv-string' )
global.client = require('simple-elasticsearch').client.create( { host : 'radiofreeinternet.com' } )

var DATA_SUBSTR_OFFSET = 5
var DATA_DIR_NAME_FILTER = "_data"

var SAVE_FILE = "./config/saved"

var saved = JSON.parse ( fs.readFileSync( SAVE_FILE ) )
if ( typeof( saved.files) == "undefined" ) 
    saved.files = { }

var config   = require( './config/config' )

function csvToElastic( file , callback){
    console.log( "file: " + file )
    var lines = fs.readFileSync( file ).toString().replace(/\r/gm,'\n').split("\n")
    var objects = new Array()
    var file_data = fs.readFileSync( file ).toString()
    var funcs = new Array()
    parse(file_data , function( err , lines ) {	
	var headers = lines[0]
	var line_count = Object.keys( lines ).length

	var country = file.match(/([a-z]*)_data/g)[0].replace("_data","")
	for( var i = 1; line_count > i; i++ ){	    
	    var columns = lines[i]
	    var column_count = Object.keys( lines[i] ).length
	    var type = columns[1].toLowerCase().replace(/[\s\t`~!@#$%^&*()_|+\-=?;:'",.<>\{\}\[\]\\\/]/gi, '')

	    for( var j = 1; column_count > j; j++){
		(function(){
		    var k = j
		    var region    = headers[j].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'').replace(/\%/,'')
		    var data      = parseInt(columns[k])

		    var timestamp = new Date( columns[0] )
		    var doc = {
			file : file,
			country : country ,
			region : region,
			data : data,
			'@timestamp' : timestamp
		    }

		    var object = { 
			index :  'ebola',
			type  : type,		
			doc : doc
		    }
		    
		    if( typeof( data ) != undefined && data !="" && data !=null )
			funcs.push( function( callback ){
			    global.client.core.index( object ,callback )
			})
		})()		
	    }
	}
	async.parallel ( funcs , function( e , r ){
	    console.log( Object.keys( r ).length + " insertions" )
	    return callback ( e , r )
	})
			
    })
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
	    console.log( "__" )
	    var csv_funcs = new Array()
	    for( var i=0; Object.keys( data_files ).length > i; i++)
		for( var j=0; Object.keys( data_files[i] ).length > j; j++){
		    (function(){
			var x = i
			var y = j
			var file = data_files[x][y]
			csv_funcs.push( function( callback ) {			    
			    csvToElastic( file , callback )
			})
		    })()
		}

	    async.series( csv_funcs, function( err , results ) {
		console.log( "Done ")
		fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))

	    })	    
	})
    })
})

