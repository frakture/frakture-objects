/*!
* frakture-api by Chris Lundberg
* Copyright 2013 Frakture, Inc.
* http://www.apache.org/licenses/LICENSE-2.0.txt
*/

if(!Frakture){
	var Frakture = Frakture || {
	init:function(opts){
	},
	notify:{
		warning:function(m){console.log(m);},
		error:function(m){alert(m);}
	}
	}
}

if (!Frakture.DB){

Frakture.DB = {
	ObjectId:function(id) {
    	//don't double ID the ObjectId if it already is one
	    if (!id) {return null;}else if (typeof id == "object" && id["$oid"] != null) {
    	    return id;
	    }

    	return {$oid: id};
	},
	//Collection class
	Collection : function(collection_name) {
   		 this.name = collection_name;
	},
	//Method to return an instance of a Collection class
	collection : function(collectionName, opts) {
		if (!collectionName) {
			Frakture.notify.error("No collection name specified to create a colleciton");
			return;
		}
		
		if (this[collectionName]) return this[collectionName];
		
		this[collectionName] = new Frakture.DB.Collection(collectionName);
		return this[collectionName];
	}
}

Frakture.DB.Collection.prototype.setGlobal = function(g) {
    this.useGlobal = g;
};

Frakture.DB.Cursor = function(collection) {
    if (!collection) { throw "collection is a required"; }
    this.collection = collection;
};

Frakture.DB.Cursor.prototype.skip = function(a) {
    this._skip = a;
    return this;
};
Frakture.DB.Cursor.prototype.limit = function(a) {
    this._limit = a;
    return this;
};
Frakture.DB.Cursor.prototype.sort = function(a) {
    this._sort = a;
    return this;
};


Frakture.DB.Cursor.prototype.toArray = function(callback,opts){
    opts = opts || {};
	if (!callback || callback.length<2){throw "Error -- toArray requires a callback with at least 2 (err and data) arguments";}
    this.executeFind(callback,{
        async: opts.async !== false
    });
};

Frakture.DB.Cursor.prototype.forEach = function(callback,opts) {
	opts = opts || {};
	if (!callback || callback.length<2){throw "Error -- toArray requires a callback with at least 2 (err and data) arguments";}

    function _callbackArray(err,arr){
		for (i in arr){
			callback(err,arr[i]);
		}
    };
    
    this.executeFind(_callbackArray,{
        async: opts.async !== false
	});
};

/* Returns an object suitable for quick lookups, instead of an array */
Frakture.DB.Cursor.prototype.toMap = function(callback,opts) {
	opts = opts || {};
	if (!callback || callback.length<2){throw "Error -- toArray requires a callback with at least 2 (err and data) arguments";}

    function _callbackArray(err,arr){
    	var o={};
		for (i in arr){
			var k=arr[i].id;
			o[k]=arr[i];
		}
		callback(err,o);
    };
    
    this.executeFind(_callbackArray,{
        async: opts.async !==false
	});
}





/* 
 *
 * Available options:
 * limit, sort, fields, skip
 */

Frakture.DB.Collection.prototype.find = function(query, fields) {
    var cur = new Frakture.DB.Cursor(this);
    if (typeof query=='function'){
    	throw "Invalid first 'find' parameter -- use forEach or toArray to iterate through results";
    }
    if (query) {
        cur.query = query;
    }else{
    	cur.query={};
    }
    if (typeof fields=='function'){
    	throw "Invalid second 'find' parameter -- use forEach or toArray to iterate through results";
    }
    if (fields) {
	     cur._fields = fields;
    }
    return cur;
};


/*
	Similar to find, but executes a blocking ajax call to return the data immediately
*/
Frakture.DB.Collection.prototype.findOne = function(query, callback_or_fields, callback) {
    var cur = new Frakture.DB.Cursor(this);
    if (query) {
        cur.query = query;
    }
    if (callback_or_fields) {
        if (typeof callback_or_fields == "function") {
            cur._callback = callback_or_fields;
        } else if (typeof callback_or_fields == "object") {
            cur._fields = callback_or_fields;
        } else {
            throw "Second parameter must be a function or object";
        }
    }
    if (callback) {cur._callback = callback;}
	if (!cur._callback || cur._callback.length<2){throw "Error -- findOne requires a callback with at least 2 (err and data) arguments";}

    cur.executeFind(
		function(err,d){
			if (d==null || d.length == 0) {
				cur._callback(null,null);
			}else{
				cur._callback(null,d[0]);
			}
    	}
    ,{
        async: false
    });
};

	Frakture.DB.connect=function(opts,callback){throw "Not implemented";};
	Frakture.DB.Cursor.prototype.executeFind = function() {
		if (!this._executeFind) throw "Not implemented";
		return this._executeFind.apply(this,arguments);
	}
	
	Frakture.DB.Collection.prototype.count = function() { 
		if (!this._count) throw "Not implemented";
		return this._count.apply(this,arguments);
	}

	Frakture.DB.Collection.prototype.update = function(query, objNew, opts_or_callback,callback) {
		if (!this._update) throw "Not implemented";
		var object=this.name;
		if (query._id){ query.id=query._id; delete query._id;}
		
		var opts={};
		if (typeof opts_or_callback=='function'){
			callback=opts_or_callback;
		}else{
			opts=opts_or_callback || {};
		}
	
		function _callback(err,data){
			if (err){ console.log(err);
			}else{
				$(document).trigger("db."+object+".change",[data]) ;
			}
			if (callback) callback(err,data);
		}
		
		this._update.apply(this,[query,objNew,opts,_callback]);
	}
	
	Frakture.DB.Collection.prototype.save = function(objNew, opts_or_callback,callback) {
		if (!this._save) throw "Not implemented";
		var object=this.name;
		var opts={};
		if (typeof opts_or_callback=='function'){
			callback=opts_or_callback;
		}else{
			opts=opts_or_callback || {};
		}
		
		function _callback(err,data){
			if (err){
				 console.log(err);
			}else{
				$(document).trigger("db."+object+".change",[data]);
			}
			
			if (callback) callback(err,data);
		}

		this._save.apply(this,[objNew,opts,_callback]);
	}

	Frakture.DB.Collection.prototype.remove = function(query, opts_or_callback,callback) {
		if (!this._remove) throw "Not implemented";
		var object=this.name;
		
		if (query._id){ query.id=query._id; delete query._id;}
		
		var opts={};
		if (typeof opts_or_callback=='function'){
			callback=opts_or_callback;
		}else{
			opts=opts_or_callback || {};
		}
		
		function _callback(err,data){
			if (err){ 
				console.log(err);
			}else{
				$(document).trigger("db."+object+".change",[data]) ;
			}
			if (callback) callback(err,data);
		}

		this._remove.apply(this,[query,opts,callback]);
	}
	
}
