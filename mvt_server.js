const compression = require('compression');
const express = require('express');
const multer  = require('multer');
const app = express();
const Pool = require('pg').Pool;
const SQL = require('sql-template-strings');
const fs = require('fs-extra');
const unzip = require('unzip');
const ogr2ogr = require('ogr2ogr')
const g_config = require(__dirname + '/config/config.json');

process.on('unhandledRejection', function(e) {
	console.log(e.message, e.stack);
})

const g_dbpools = {};
for (db in g_config.databases) {
	console.log("Setting up pool for database '" + db + "'");
	g_dbpools[db] = new Pool(g_config.databases[db]);
}
const g_sources = {};
const g_sourcedir = __dirname + '/' + g_config.sourcedir + '/';
fs.readdirSync(g_sourcedir).forEach(file => {
	var matches = file.match(/^(\w+)\.json$/)
	if (!matches){
		console.log('Skipping unknown source file: ' + file);
		return;
	}
	console.log('Reading source file ' + file);
	var sourcename = matches[1];
	try {
		var json = fs.readFileSync(g_sourcedir + file, 'utf8');
		var source = JSON.parse(json);
		if (!g_dbpools.hasOwnProperty(source.database)) {
			console.log("Unknown database '" + source.database + "' in " + file);
			return;
		}
		g_sources[sourcename] = source;
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
			console.log('Invalid source id');
			code = 400;
			reponse = 'Invalid source id';
			break;
		}
		if (!g_sources.hasOwnProperty(layer)) {
			console.log('Unknown source: ' + layer);
			code = 404;
			response = 'Unknown source: ' + layer;
			break;
		}
		var source = g_sources[layer];
		var name = source.name;
		var pool = g_dbpools[source.database];
		
		var attributes = source.attributes ? ', ' + source.attributes : '';
		var join = source.join ? source.join : '';
		var groupby = source.groupby ? source.groupby : '';
		
		var query_text = `
		SELECT ST_AsMVT('${name}', 4096, 'geom', q) AS mvt FROM (
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

var allowCORS = function(req, res, next) {
    res.setHeader('Access-Control-Allow-Origin', '*');
//    res.setHeader('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE');
//    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    next();
}
app.use(allowCORS);
app.use('/html', express.static(__dirname + '/html'));

function unzipUpload(zipfile) {
	return new Promise(function(resolve, reject) {
		var output_dir = zipfile + '_unzip';
		if (!fs.existsSync(output_dir)){
			fs.mkdirSync(output_dir);
		}
		var result = '';
		fs.createReadStream(zipfile)
			.pipe(unzip.Parse())
			.on('entry', function (entry) {
				var fileName = entry.path;
				var type = entry.type;
//				console.log('entry ' + fileName);
				if (!result && type == 'File' && fileName.match(/^.+\.(json|geojson)$/)) {
//					console.log('unzipping ' + fileName);
					entry.pipe(fs.createWriteStream(output_dir + '/' + fileName));
					result = output_dir + '/' + fileName;
				} else {
					entry.autodrain();
				}
			})
			.on('close', function () {
				console.log('closed zip');
				if (!result) {
					reject('No geojson found in zip file'); return;
				}
				resolve(result);
			});
	});
}

function loadGeoJson(file) {
	var promises = [];
	var database = g_config.uploads.database;
	var host = g_config.databases[database].host;
	var port = g_config.databases[database].port;
	var user = g_config.databases[database].user;
	var pass = g_config.databases[database].password;
	var dbname = g_config.databases[database].database;
	var schema = g_config.uploads.schema;
	var pgstr = `PG:host=${host} port=${port} user=${user} dbname=${dbname}`;
	if (pass) pgstr = pgstr + ` password=${pass}`;
	
	return new Promise( function(resolve, reject) {
		console.log('loading geojson');
		var matches = file.match(/\/(\w+)_unzip\/[^\/]+$/);
		var table = matches[1];
		ogr2ogr(file)
			.format('PostgreSQL')
			.options(['-nln', schema + '.' + table])
			.destination(pgstr)
			.timeout(60000)
			.exec(function(err) {
				if (err) {
					reject(err.message); return;
				}
				resolve(table);
			});
	});
}

function makeConfig(table) {
	return new Promise(function(resolve, reject) {
		var schema = g_config.uploads.schema;
		var pool = g_dbpools[g_config.uploads.database];
		//var query = `SELECT * FROM ${table} WHERE false`;
		var query = `SELECT column_name, udt_name FROM INFORMATION_SCHEMA.COLUMNS 
						WHERE table_schema = '${schema}' AND table_name = '${table}'`;
		pool.query(query).then(result => {
			var geom_column = '';
			var fields = result.rows.filter(row => {
				if (row.udt_name == 'geometry') {
					geom_column = row.column_name;
					return false;
				} else {
					return true;
				}
			}).map(row => {
				return row.column_name;
			});
			if (geom_column) {
				pool.query(`SELECT Find_SRID('${schema}', '${table}', '${geom_column}') srid, 
							GeometryType(${geom_column}) geom_type 
							FROM ${schema}."${table}" LIMIT 1`).then(result => {
					var srid = result.rows[0].srid;
					var geom_type = result.rows[0].geom_type;
					var source_config = {
						"database": g_config.uploads.database,
						"table": `${schema}."${table}"`,
						"geometry": geom_column,
						"type": geom_type,
						"srid": srid,
						"minzoom": 0,
						"maxzoom": 22,
						"attributes": fields.join(', ')
					};
					var json = JSON.stringify(source_config, null, '\t');
					
					fs.writeFile(g_sourcedir + table + '.json', json, err => {
						if (err) {
							reject('Error writing source file for id: ' + table);
						} else {
							g_sources[table] = source_config;
							resolve('Successfully uploaded json with id: ' + table);
						}
					});
				});
			} else {
				resolve('Unable to find geometry column');
			}
		}).catch(reason => {
			reject('Error executing database query: ' + reason.message);
		});
	});
}

function makeStyle(id){
	return new Promise(function(resolve, reject) {
		var source = g_sources[id];
		var pool = g_dbpools[source.database];
		var query = `WITH extent AS (
						SELECT ST_Transform(ST_SetSRID(ST_Extent(wkb_geometry), 28992), 4326) geom FROM ${source.table}
					)
					SELECT ST_XMin(geom) xmin, ST_YMin(geom) ymin, ST_XMax(geom) xmax, ST_XMax(geom) ymax FROM extent`;
		pool.query(query).then(result => {
			var row = result.rows[0];
			var xrange = row.xmax - row.xmin;
			var yrange = row.ymax - row.ymin;
			var center = [row.xmin + xrange / 2, row.ymin + yrange / 2];
			var zoom = Math.min(1-Math.log2(xrange/360), 1-Math.log2(yrange/180)) + 1;
			
			
		});
	});
}

const upload = multer({ dest: __dirname + '/' + g_config.uploads.directory });
app.post('/upload', upload.single('file'), function (req, res, next) {
	//console.log(req.file);
	function cleanup(path) {
		fs.remove(path);
		fs.remove(path + '_unzip');
	}
	if (req.file.mimetype == 'application/x-zip-compressed') {
		unzipUpload(req.file.path)
			.then(loadGeoJson)
			.then(makeConfig)
			.then( result => {
				console.log(result);
				res.status(200)
					.send(result);
				cleanup(req.file.path);
			})
			.catch( reason => {
				console.log('Error loading geojson: ' + reason);
				res.status(400)
					.send('Error loading geojson: ' + reason);
				cleanup(req.file.path);
			});
	} else {
		res.status(400);
		res.send('Please upload a zipped geojson.');
		cleanup(req.file.path);
	}
});


/*app.get('/upload.html', function(req, res) {
	fs.readFile(__dirname + '/upload.html', function (err, html) {
		if (err) {
			res.status(500);
			res.send('Error loading upload page');
		} else {
			res.status(200);
			res.header('Content-Type', 'text/html');
			res.send(html);
		}
	});	
});*/

app.listen(g_config.port);
console.log('mvt server is listening on port ' + g_config.port);
