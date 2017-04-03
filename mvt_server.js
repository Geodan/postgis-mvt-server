var compression = require('compression');
var express = require('express');
var app = express();
var Pool = require('pg').Pool;
const SQL = require('sql-template-strings');

process.on('unhandledRejection', function(e) {
	console.log(e.message, e.stack);
})

var pgconfig = {
	host: 'localhost',
	user: 'postgres',
	password: '',
	database: 'research',
};
var pool = new Pool(pgconfig);

var sources = {
	'terrorism': {
		'table': 'globalterrorismdb_0616',
		'geometry': 'shape',
		'type': 'point',
		'srid': 4326,
		'minzoom': 0,
		'maxzoom': 19,
		'fields': 'nkill Casualties',
		'groupby': ''
	},
	'nwb': {
		'table': 'nwb_2016_01.weg_wegvakken',
		'geometry': 'geom',
		'type': 'polygon',
		'srid': 28992,
		'minzoom': 6,
		'maxzoom': 13,
		'fields': ''
	},
	'woonplaatsen': {
		'table': 'bagagn_201702.woonplaats',
		'geometry': 'geom',
		'type': 'polygon',
		'srid': 28992,
		'minzoom': 6,
		'maxzoom': 13,
		'fields': 'woonplaats Woonplaats'
	},
	'buurten': {
		'table': 'cbs.buurten',
		'geometry': 'wkb_geometry',
		'type': 'polygon',
		'srid': 28992,
		'minzoom': 6,
		'maxzoom': 13,
		'fields': 'buurtnaam Buurt, a_inw Inwoners'
	},
	'gebouwen': {
		'table': 'bagagn_201702.gebouwen',
		'geometry': 'geom',
		'type': 'polygon',
		'srid': 28992,
		'minzoom': 14,
		'maxzoom': 20,
		'fields': 'bouwjaar Bouwjaar, oppvlakte Oppervlakte'
	},
	'wegen': {
		'table': 'bgt.wegdeel_2d',
		'geometry': 'wkb_geometry',
		'type': 'polygon',
		'srid': 28992,
		'minzoom': 14,
		'maxzoom': 20,
		'fields': 'bgt_functie Functie'
	}
};

app.get('/mvt/:layer/:z/:x/:y.mvt', function(req, res) {
	var x = parseInt(req.params.x);
	var y = parseInt(req.params.y);
	var z = parseInt(req.params.z);  
	var layer = req.params.layer;
	if (!sources.hasOwnProperty(layer)) {
		console.log('Invalid source: ' + layer);
		res.status(500).send('Invalid source: ' + layer);
		return;
	}
	var source = sources[layer];

	var onError = function(err) {
		console.log(err.message, err.stack,query)
		res.status(500).send('An error occurred');
	};

	var fields = source.fields ? ', ' + source.fields : '';
	var join = source.join ? source.join : '';
	var groupby = source.groupby ? source.groupby : '';
	
	var query = `
	SELECT ST_AsMVT('${layer}', 4096, 'geom', q) AS mvt FROM (
		SELECT ST_AsMVTGeom(ST_Transform(${source.table}.${source.geometry}, 3857), TileBBox(${z}, ${x}, ${y}, 3857), 4096, 10, true) geom ${fields}
		FROM ${source.table}
		${join}
		WHERE ST_Intersects(TileBBox(${z}, ${x}, ${y}, ${source.srid}), ${source.table}.${source.geometry})
		${groupby}
	) AS q
	`;
	//console.log(query);
	pool.query(query, function(err, result) {
		if (err) return onError(err);
		res.status(200).header('content-type', 'application/octet-stream');
		res.header("Access-Control-Allow-Origin", "*");
		res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
		res.send(result.rows[0].mvt);
	});	
});

app.listen(3001);
console.log('mvt server is listening on 3001')
