var compression = require('compression');
var express = require('express');
var app = express();
var Pool = require('pg').Pool;
const SQL = require('sql-template-strings');
var fs = require('fs');
var config = require(__dirname + '/config.json');

process.on('unhandledRejection', function(e) {
	console.log(e.message, e.stack);
})

var dbpools = {};
for (db in config.databases) {
	console.log("Setting up pool for database '" + db + "'");
	dbpools[db] = new Pool(config.databases[db]);
}
var sources = {};
var sourcedir = __dirname + '/' + config.sourcedir + '/';
fs.readdirSync(sourcedir).forEach(file => {
	var matches = file.match(/^(\w+)\.json$/)
	if (!matches){
		console.log('Skipping unknown source file: ' + file);
		return;
	}
	console.log('Reading source file ' + file);
	var sourcename = matches[1];
	try {
		var json = fs.readFileSync(sourcedir + file, 'utf8');
		var source = JSON.parse(json);
		if (!dbpools.hasOwnProperty(source.database)) {
			console.log("Unknown database '" + source.database + "' in " + file);
			return;
		}
		sources[sourcename] = source;
	} catch (err) {
		console.log('Invalid source file: ' + file);
	}
});

app.get('/mvt/:layers/:z/:x/:y.mvt', function(req, res) {
	var x = parseInt(req.params.x);
	var y = parseInt(req.params.y);
	var z = parseInt(req.params.z);
	var code = 0;
	var response = '';
	var layers = req.params.layers.split(',');
	
	var queries = [];
	for (var i=0; i<layers.length; i++) {
		var layer = layers[i];
		if (!layer.match(/^\w+$/)){
			console.log('Invalid source name: ' + layer);
			code = 400;
			reponse = 'Invalid source name: ' + layer;
			break;
		}
		if (!sources.hasOwnProperty(layer)) {
			console.log('Unknown source: ' + layer);
			code = 404;
			response = 'Unknown source: ' + layer;
			break;
		}
		var source = sources[layer];
		var pool = dbpools[source.database];
		
		var attributes = source.attributes ? ', ' + source.attributes : '';
		var join = source.join ? source.join : '';
		var groupby = source.groupby ? source.groupby : '';
		
		var query_text = `
		SELECT ST_AsMVT('${layer}', 4096, 'geom', q) AS mvt FROM (
			SELECT ST_AsMVTGeom(ST_Transform(${source.table}.${source.geometry}, 3857), TileBBox(${z}, ${x}, ${y}, 3857), 4096, 10, true) geom ${attributes}
			FROM ${source.table}
			${join}
			WHERE ST_Intersects(TileBBox(${z}, ${x}, ${y}, ${source.srid}), ${source.table}.${source.geometry})
			${groupby}
		) AS q where q.geom is not null
		`;
		//console.log(query_text);
		queries.push(pool.query(query_text));
	}
	
	if (code) { // an error occured
		res.status(code);
		res.send(response);
	} else {
		Promise.all(queries).then(values => {
			if (values.length == 0) {
				response = '';
			} else if (values.length == 1) {
				response = values[0].rows[0].mvt;
			} else if (values.length > 1) {
				var buffers = [];
				values.forEach(val => {
					if (val.rows[0].mvt) {
						buffers.push(new Buffer(val.rows[0].mvt));
					}
				});
				if (buffers.length > 0)	response = Buffer.concat(buffers);
			}
			if (response) {
				res.status(200);
			} else {
				res.status(204); // no content
			}
			res.header('content-type', 'application/octet-stream');
			res.header("Access-Control-Allow-Origin", "*");
			res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
			res.send(response);
		}).catch(reason => {
			console.log('Error executing database query: ' + reason.message, reason.stack);
			res.status(500);
			res.send('An error occurred');
		});
	}
});

app.listen(config.port);
console.log('mvt server is listening on port ' + config.port);
