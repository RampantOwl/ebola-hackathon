var fs       = require( 'fs' )
var util     = require( 'util' )
var path     = require( 'path' )
var util     = require( 'util' )
var async    = require( 'async' )
var winston  = require('winston')

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
    
    var country = file.match(/([a-z]*)_data/g)[0].replace("_data","")


    var headers = lines[0].toLowerCase().split( ',' )
    
    var line_count = Object.keys( lines ).length
    var objects = new Array()

    for( var i = 1; line_count > i; i++ ){
	if( typeof( lines[i] ) == "undefined" )
	    continue
	var line_data = lines[i].split( ',' )
	var line_data_count = Object.keys( line_data ).length
	for( var j = 2; line_data_count > j; j++ ){
	    if( typeof(line_data[j]) == "undefined" || line_data[j] == '' || line_data == null)
		continue
	    var type = line_data[1].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'')
	    if( typeof( headers[j] ) == "undefined" ){
		var name  = "na"
	    }else{
		var name = headers[j].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'')
	    }

	    var doc = {
		country : country ,
		region: name,
		data : line_data[j],
		'@timestamp' : new Date( line_data[0] )
	    }
	    var object = { 
		index : 'ebola',
		type  : type,
		
		doc : doc
	    }
	}
	console.log( util.inspect( object , { depth : null } ) )
	global.client.core.index( object , function( e ,r ){ } )
	objects.push ( object )
	//console.log( lines[i] )
    }
    console.log( Object.keys( objects ).length + " objects created" )
    callback( null , objects )
    /*      
    for( var i=1; lines > i; i++){

	//var line_data = data[i].toLowerCase().replace(/\s/g,'_').replace(/`/g,'').replace(/'/g,'').split(',')
	console.log( data[i] )
	var line_data = data[i].split(',')

	var datetime = new Date( line_data[0] )

	var key_length = Object.keys( keys ).length
	var obj = {}
	var es_funcs = []

	console.log( line_data )
	for( var k =2; key_length > k; k++ ){
	    (function(){
		var n = k
		console.log( line_data[n] )
		var type = line_data[n][1].replace(/\s/gm,'_')
		var doc = { }
		var header_index = 0
		console.log( type )    
		//console.log( "type: " + type )
		    doc['region'] = { name : keys[n] , value : line_data[n] }
		    doc['@timestamp'] = datetime
		    
		    es_funcs.push( function( callback ){
			
		    
		    global.client.core.index( { index: country , type : type , doc : doc } , function( e ,r ){
			    console.log( "Inserted : " + Object.keys ( r ).length )
			    callback( e, r )
			})
		    })
	    })()
	}
    }
    
    async.series( es_funcs , callback )
    */
    
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
		//console.log( results )
	    })
	    
	    //fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
	})
	//fs.writeFileSync( SAVE_FILE , JSON.stringify( saved ))
    })
    
})

