var compression = require('compression');
var express = require('express');
var app = express();
var Pool = require('pg').Pool;
const SQL = require('sql-template-strings');

function tile2long(x,z) {
	return (x/Math.pow(2,z)*360-180);
}
function tile2lat(y,z) {
	var n=Math.PI-2*Math.PI*y/Math.pow(2,z);
	return (180/Math.PI*Math.atan(0.5*(Math.exp(n)-Math.exp(-n))));
}
 
var pgconfig = {
	host: 'localhost',
	user: 'postgres',
	password: '',
	database: 'research',
};

process.on('unhandledRejection', function(e) {
	console.log(e.message, e.stack);
})

var pool = new Pool(pgconfig);
//app.use(compression()); //Compression seems to slow down significantly and wouldn't add much

app.get('/bgt/:layer/:z/:x/:y.mvt', function(req, res) {
	var layer = (req.params.layer);
	var x = parseInt(req.params.x);
	var y = parseInt(req.params.y);
	var z = parseInt(req.params.z);  

	var minx = tile2long(x,z);
	var maxx = tile2long(x+1,z);
	var miny = tile2lat(y+1,z);
	var maxy = tile2lat(y,z);
	
	switch (layer){
		case 'ondersteunendwaterdeel_2d':
		case 'scheiding_2d':
		case 'waterdeel_2d':
			var col = 'bgt_type';
			break;
		case 'begroeidterreindeel_2d':
		case 'onbegroeidterreindeel_2d':
			var col = 'bgt_fysiekvoorkomen';
			break;
		case 'ondersteunendwegdeel_2d':
		case 'wegdeel_2d':
			var col = 'bgt_functie';
			break;
		default:
			var col = 'bgt_status';
	}
				
	
	var query = SQL`
	WITH bbox AS (
		SELECT ST_Transform(ST_MakeEnvelope(${minx},${miny},${maxx},${maxy}, 4326), 28992) AS geom
	)
	,items AS (
		SELECT ST_Transform(wkb_geometry, 4326) geom, lokaalid as id, ${col} as kind 
		FROM bgt.${layer} , bbox
		WHERE ST_Intersects(bbox.geom, wkb_geometry)
		AND relatievehoogteligging = 0
		LIMIT 1000 --Security
	)
	SELECT ST_AsMVT('${layer}', ST_MakeBox2D(ST_Point(${minx},${miny}), ST_Point(${maxx},${maxy})), 4096, 0, false, 'geom', q) mvt 
	FROM items AS q
	`;

	var onError = function(err) {
		console.log(err.message, err.stack,query)
		res.status(500).send('An error occurred');
	};

	pool.query(query, function(err, result) {
		if (err) return onError(err);
		res.status(200).header('content-type', 'application/octet-stream');
		res.header("Access-Control-Allow-Origin", "*");
		res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
		var mvt = result.rows[0].mvt;
		res.send(mvt);
	});
});

app.listen(3001);
console.log('server is listening on 3001')
