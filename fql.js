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
	var obj3 = {};
	for (var key in obj1){
		obj3[key] = obj1[key];
	}
	for (key in obj2) {
		obj3[key] = obj2[key];
	}
	return obj3;
}

function FQL (table) {
	this.data = table;
	this.indexTables = {};
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
	var newData = [];
	var keys = Object.keys(obj);

	if (keys.length === 1 && this.indexTables[keys[0]]) {
		var indices = this.getIndicesOf(keys[0], obj[keys[0]]);
		//ForEach call changes this to global (or nothing in strict)
		//forEach takes an optional thisArg after callback
		indices.forEach(function(index) {
			newData.push(this.data[index]);
		}, this);
		return new FQL(newData);
	}

	newData = this.data.filter(function(movie) {

		for (var key in obj) {
			if(typeof obj[key] !== 'function'){

				if ( movie[key] !== obj[key] ) {
					return false;
				}
			}else{
				if(!obj[key](movie[key])) return false;
			}
		}
		return true;
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
				newData.push(merge(movie,role));
			}
		});
	});

	return new FQL(newData);
};

FQL.prototype.addIndex = function(key){
	var indexTable = {};
	//Iterate through data
	//for each row, create array for that index if there isn't already one
	//if that index exists in indexTable push table array index to index array
	this.data.forEach(function(movie,i){
		if(!indexTable[movie[key]]) indexTable[movie[key]] = [i];
		else indexTable[movie[key]].push(i);
	});

	this.indexTables[key]=indexTable;
	return this;
};

FQL.prototype.getIndicesOf = function(key,value){
	if(this.indexTables[key]) return this.indexTables[key][value];
	return undefined;
};














