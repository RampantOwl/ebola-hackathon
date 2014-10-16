var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )
var util     = require( 'util' )
var async    = require( 'async' )

var parse    = require( 'csv-parse' )
var recursive = require( 'recursive-readdir' )

var SAVE_FILE = "./config/saved"

var saved = JSON.parse ( fs.readFileSync( SAVE_FILE ) )
if ( typeof( saved.files) == "undefined" ) 
    saved.files = { }

var config   = require( './config/config' )

global.client = require('simple-elasticsearch').client.create( { host : config.elastic_host } )

function cleanField( dirty_field ){    
    return dirty_field.toLowerCase().replace(/[\s\t`~!@#$%^&*()_|+\-=?;:'",.<>\{\}\[\]\\\/]/gi, '')
}

/* Try to grok a country name from somewhere in the file path*/
function getCountryFromFilepath( filepath ){
    
    var file_path_seperated = filepath.split(path.sep)
    var file_dir = file_path_seperated[file_path_seperated.length -2]
    var file_name = file_path_seperated[file_path_seperated.length -1]
    
    var country = file_dir.substr(0,file_dir.indexOf("_data"))
    
    if( typeof( country ) == "undefined" || country=="" || country==null)
	country = file_name.substr(0,file_name.indexOf("_case_data"))

    if( typeof( country ) == "undefined" || country=="" || country==null)
	country = "na"

    return country
}

function createDocument(file , country , region , data , stat_name , timestamp ){
    var document = {
	file : file,
	country : country ,
	region : region,
	data : data,
	stat_name : stat_name,
	'@timestamp' : timestamp
    }

    return document
}

/* Functions that creates key/value mappings for a csv file and sends it to elasticsearch*/
function csvToElastic( file , callback){
    console.log( "File: %s ", file )
    
    var objects = new Array()
    var funcs = new Array() /* Stores functions that will later be run in parallel */
    
    var file_data = fs.readFileSync( file ).toString()
    
    
    var country = getCountryFromFilepath( file )
    

    console.log( "Country Name: %s" ,country )
    /* Takes csv data and returns 2-dimentional array of its contents */
    parse(file_data , function( err , lines ) {	
	var headers = lines[0]
	var line_count = Object.keys( lines ).length
	
	/* Begin reading at line after header */
	/* Each line has data for one type of statistic, ex: contacts confirmed */
	for( var i = 1; line_count > i; i++ ){
	    var columns = lines[i]
	    var column_count = Object.keys( lines[i] ).length
	    var stat_name = cleanField(columns[1])
	    /* For each column, create a mapping of type, region and data */
	    for( var j = 2; column_count > j; j++){
		(function(){
		    var k = j /* Bind k since in closure */
		    var region    = cleanField(headers[k])
		    var data      = parseInt(columns[k])
		    var timestamp = new Date( columns[0] )
		    
		    var doc = createDocument( file , country , region , data , stat_name , timestamp )

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
	    console.log( "Attempted insertions: %s" ,Object.keys( r ).length )
	    return callback ( e , r )
	})
	
    })
}

if( typeof( config.datadir ) == "undefined" ){
    console.log( "Set datadir directive in config file." )
    proces.exit()
}
recursive( path.resolve( __dirname , config.datadir ) , function( err , files ){

    var csv_funcs = new Array()
    
    for( var i=0; files.length > i; i++){
	if( typeof( saved[files[i]] ) != "undefined" )
	    continue

	if( (files[i].indexOf('.csv') > -1 ) && (files[i].indexOf('ebola_') > -1 || files[i].indexOf('case_') > -1 || files[i].indexOf('liberia_data/') > -1 || files[i].indexOf('sl_data/') > -1)){
	    (function(){
		saved[files[i]] = true		
		var j = i
		var file = files[i]
		csv_funcs.push( function( callback ) {			    
		    csvToElastic( file , callback )
		})
	    })()
	}
    }

    console.log( "Parsing %s csv files", csv_funcs.length )
    async.series( csv_funcs, function( err , results ) {
	console.log( "Files processed: %s", results.length )
	/* Save the files that have been read*/
	fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
	console.log( "Saved %s" , SAVE_FILE )
    })	    
})

