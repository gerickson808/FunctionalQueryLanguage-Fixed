var fs = require('fs');

// _readTable takes a string representing a table name
// and returns an array of objects, namely the rows.
// It does so by looking up actual files, reading them,
// and parsing them from JSON strings into JS objects.
function _readTable (tableName) {
	var folderName = __dirname + '/film-database/' + tableName;
	var fileNames = fs.readdirSync(folderName);
	var fileStrings = fileNames.map(function (fileName) {
		var filePath = folderName + '/' + fileName;
		return fs.readFileSync(filePath).toString();
	});
	var table = fileStrings.map(function (fileStr) {
		return JSON.parse(fileStr);
	});
	return table;
}

function merge (obj1, obj2) {
	for (var key in obj2) {
		obj1[key] = obj2[key];
	}
	return obj1;
}

function FQL (table) {
	this.data = table;
}

module.exports = {
	FQL: FQL,
	merge: merge,
	_readTable: _readTable
};

FQL.prototype.exec = function(){
	return this.data;
};

FQL.prototype.count = function() {
	return this.data.length;
};

FQL.prototype.limit = function(num){
	var newData = this.data.slice(0,num);
	return new FQL(newData);
};

FQL.prototype.where = function(obj) {
	var newData = this.data.filter(function(movie) {
		var found = true;
		for (var key in obj) {
			if(typeof obj[key] !== 'function'){
				if ( movie[key] !== obj[key] ) {
					found = false;
				}
			}else{
				if(!obj[key](movie[key])) found = false;
			}
		}
		return found;
	});
	return new FQL(newData);
};

FQL.prototype.select = function(properties) {
	var newData = this.data.map(function(movie) {
		var selectedObj = {};
		properties.forEach(function(prop) {
			selectedObj[prop] = movie[prop];
		});
		return selectedObj;
	});


	return new FQL(newData);
};

FQL.prototype.order = function(string){
	var newData = this.data.sort(function(a,b){
		return a[string] - b[string];
	});
	return new FQL(newData);
};

FQL.prototype.left_join = function(fql, func) {
	var newData = [];
	this.data.forEach(function(movie) {
		fql.data.forEach(function(role) {
			if(func(movie, role)) {
				// Make copy of movie so we're not overwriting actual movie object
				var movieCopy = {};
				for (var key in movie) {
					movieCopy[key] = movie[key];
				}

				for (var key in role) {
					movieCopy[key] = role[key];
				}
				newData.push(movieCopy);
			}
			
		});
	});

	return new FQL(newData);
};