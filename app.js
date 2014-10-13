var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )
var util     = require( 'util' )
var async    = require( 'async' )

var parse    = require( 'csv-parse' )
var recursive = require( 'recursive-readdir' )

global.client = require('simple-elasticsearch').client.create( { host : 'radiofreeinternet.com' } )

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
    var country = file.replace(/_data$/,'')    
    
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

recursive( path.resolve( __dirname , config.datadir ) , function( err , files ){
    var csv_funcs = new Array()
    
    for( var i=0; files.length > i; i++){
	if( (files[i].indexOf('.csv') > -1 ) && (files[i].indexOf('ebola_') > -1 || files[i].indexOf('case_') > -1 ) ){
		(function(){
		    if( typeof( saved[files[i]] ) != "undefined" )
			return		    
		    saved[files[i]] = true		
		    var j = i
		    var file = files[i]
		    csv_funcs.push( function( callback ) {			    
			csvToElastic( file , callback )
		    })
		})()
	}
    }
    async.series( csv_funcs, function( err , results ) {
	console.log( "Done ")	
	/* Save the files that have been read*/
	fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))

    })	    
})

