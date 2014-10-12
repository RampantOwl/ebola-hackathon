var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )
var util     = require( 'util' )
var async    = require( 'async' )

var parse    = require( 'csv-parse' )

global.client = require('simple-elasticsearch').client.create( { host : 'radiofreeinternet.com' } )

var DATA_DIR_NAME_FILTER = "_data"

var SAVE_FILE = "./config/saved"

var saved = JSON.parse ( fs.readFileSync( SAVE_FILE ) )
if ( typeof( saved.files) == "undefined" ) 
    saved.files = { }

var config   = require( './config/config' )

/* Functions that creates key/value mappings for a csv file and sends it to elasticsearch*/
function csvToElastic( file , callback){
    console.log( "Processing " + file )
    
    var objects = new Array()
    var funcs = new Array() /* Stores functions that will later be run in parallel */
    
    var file_data = fs.readFileSync( file ).toString()
    var country = file.match(/([a-z]*)_data/g)[0].replace("_data","")
    
    /* Takes csv data and returns 2-dimentional array of its contents */
    parse(file_data , function( err , lines ) {	
	var headers = lines[0]
	var line_count = Object.keys( lines ).length
	
	/* Begin reading at line after header */
	/* Each line has data for one type of statistic, ex: contacts confirmed */
	for( var i = 1; line_count > i; i++ ){
	    var columns = lines[i]
	    var column_count = Object.keys( lines[i] ).length
	    var stat_name = columns[1].toLowerCase().replace(/[\s\t`~!@#$%^&*()_|+\-=?;:'",.<>\{\}\[\]\\\/]/gi, '')
	    /* For each column, create a mapping of type, region and data */
	    for( var j = 2; column_count > j; j++){
		(function(){
		    var k = j /* Bind k since in closure */
		    var region    = headers[k].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'').replace(/\%/,'')
		    var data      = parseInt(columns[k])
		    var timestamp = new Date( columns[0] )
		    
		    var doc = {
			file : file,
			country : country ,
			region : region,
			data : data,
			stat_name : stat_name,
			'@timestamp' : timestamp
		    }

		    var object = { 
			index :  'ebola',
			type  : 'stat',		
			doc : doc
		    }
		    
		    /* If there was any data for this line, store a function that sends this object in funcs*/
		    if( typeof( data ) != "undefined" && data !="" && data !=null && isNaN(data)==false)
			funcs.push( function( callback ){
			    global.client.core.index( object ,callback )
			})
		})()		
	    }
	}
	
	/* Run all the insert functions in parallell */
	async.parallel ( funcs , function( e , r ){
	    console.log( Object.keys( r ).length + " insertions" )
	    return callback ( e , r )
	})
	
    })
}
fs.readdir( path.resolve( __dirname , config.datadir ), function( err , files ) {
    var count     = Object.keys( files ).length
    var funcs =  new Array()

    /* Create functions that will read the data directory */
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

    /* Run the functions in a series*/
    async.series( funcs , function( err , results ) {
	var read_funcs = new Array()
	var results_count = Object.keys( results ).length
	/* For each file found, create a list of functions that will read those files unless the file has been read */
	for( var i = 0; results_count > i; i++ ){
	    var resolved_path = results[i]
	    var stats = fs.statSync( resolved_path )
	    /* Only read if it is a directory matching the country */
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
	/* Iterate over the files that were found, and create a list of functions that will run csvToElastic */
	async.series( read_funcs, function( err , data_files ) {	    
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
		
		/* Save the files that have been read*/
		fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))

	    })	    
	})
    })
})

