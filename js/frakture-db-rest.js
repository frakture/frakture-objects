/*!
* frakture-api by Chris Lundberg
* Copyright 2013 Frakture, Inc.
* http://www.apache.org/licenses/LICENSE-2.0.txt
*/
Frakture.DB.connect=function(opts,callback){
	callback(null,Frakture.DB);
}

//Determine the path to this file, no matter where it's mounted
Frakture.DB.path=null;
(function(){
	var scripts = document.getElementsByTagName("script");
	var src = scripts[scripts.length-1].src;
	Frakture.DB.path=$("<a/>").attr("href",src)[0].pathname.split("/").slice(0,-2).join("/");
})();

//Extend Frakture.DB to override certain functions for RESTFUL calls
Frakture.DB.restSuccess=function(d,s,x){
	if (this._callback){this._callback(null,d)}
}
Frakture.DB.restError=function(xhr){
	var err=null;
	if (xhr.responseText && xhr.readyState) {
		try {
			err = JSON.parse(xhr.responseText);
		} catch(e) {
			console.log("Warning, could not parse JSON response:" + xhr.responseText);
		}
	}
	err=err || {ok:0,error:"No error available"};
	
	if (this._callback){this._callback(err);}else{console.log(err);}
}

/*
	signature of the callback should be function(err,data)
*/
Frakture.DB.Cursor.prototype._executeFind = function(callback,opts) {
    opts=opts || {};
    var async = opts.async === true || false;

    var data = {};
    
    if (typeof this.query!='object') throw "query must be an object";
    data.q = JSON.stringify(this.query);
    
	if (this._limit) data.limit = this._limit;
	if (this._sort) data.sort = JSON.stringify(this._sort);
    if (this._skip) data.skip = this._skip;
    if (this._skip && !this._sort) throw "skipping entries requires a 'sort'";
    if (this._fields) data.fields = JSON.stringify(this._fields);
    if (this.collection.useGlobal) data.useGlobal = true;

    $.ajax({
        url: Frakture.DB.path+"/"+this.collection.name,
        type: "get",
        data: data,
        dataType: "json",
        async: async,
        success: Frakture.DB.restSuccess,
        error: Frakture.DB.restError, _callback:callback
    });
};

/*
opts:{
		async -- default true
	}
*/
Frakture.DB.Collection.prototype._count = function(query, opts_or_callback,callback) {
    var data = {};
    
    if (typeof query == "object") {
        data.q = JSON.stringify(query);
    } else {
        throw "First parameter must be a query object " + typeof(query);
    }
    
    var opts={};
	if (typeof opts_or_callback=='function'){
		callback=opts_or_callback;
	}else{
		opts=opts_or_callback || {};
	}
    if (this.useGlobal) { data.useGlobal = true;}

    $.ajax({
        url: Frakture.DB.path+"/"+this.name+"/count",
        type: "get",
        data: data,
        dataType: "json",
        async: opts.async !== false,
        success: Frakture.DB.restSuccess,
        error: Frakture.DB.restError, _callback:callback
    });
};

/* 
Similar to Mongo calls
query: A Javascript object containing the query of which item(s) to update, or just a string ID
objNew:  A Javascript object with the new data, or data modifiers
opts:{
		upsert - if this should be an "upsert"; that is, if the record does not exist, insert it
		multi - if all documents matching query should be updated
		async -- default true, indicates whether the method should be run asynchronously
		success - Javascript function of what to do on a successful update
		failure - Javascript function of what to do on a failed update
	}
*/
Frakture.DB.Collection.prototype._update = function(query, objNew, opts,callback) {
	if (!query || typeof query !='object') {
        Frakture.notify.error("A query object is required for an update");
        return;
    }
    
    if ("_id" in query && query._id===undefined) throw "db-rest update: query contains an _id that is not defined.  Please remove _id or provide a valid _id.";
    
	
    var data = {};
    if (this.useGlobal) { data.useGlobal = true;}

    data.q = JSON.stringify(query);
    data.data = JSON.stringify(objNew);
    
    if (opts && opts.multi) {
        data.multi = true;
    }
    
    if (opts && opts.upsert) {
        data.upsert = true;
    }
    
    var url=Frakture.DB.path+"/"+this.name;
    if (query._id) url+="/"+query._id;

    $.ajax({
        url: url,
        type: "PUT",
        data: data,
        dataType: "json",
        async: opts.async !== false,
        success: Frakture.DB.restSuccess,
        error: Frakture.DB.restError,
        _callback:callback
    });
};


/*
	Similar to Mongo save -- save will REPLACE existing record, not just change fields
*/
Frakture.DB.Collection.prototype._save = function(objNew, opts, callback) {
    if (!objNew) {
        Frakture.notify.error("Object required");
        return;
    }
    
    var data = {};
    if (this.useGlobal) {data.useGlobal = true;}
    
    
    data.data = JSON.stringify(objNew);
    $.ajax({
        url: Frakture.DB.path+"/"+this.name,
        type: "POST",
        data: data,
        dataType: "json",
        async: opts.async !== true,
        success: Frakture.DB.restSuccess,
        error: Frakture.DB.restError, 
        _callback:callback
    });
};

/*
	Remove data
*/

Frakture.DB.Collection.prototype._remove = function(query, opts,callback) {
	if (!query || typeof query !='object') {
        Frakture.notify.error("A query object is required for a remove");
        return;
    }

    var data = {};
    data.q = JSON.stringify(query);
    
   if (this.useGlobal) { data.useGlobal = true; }

    var url=Frakture.DB.path+"/"+this.name;
    if (query._id) url+="/"+query._id.toString();

    $.ajax({
        url: url,
        type: "DELETE",
        data: data,
        dataType: "json",
        async: opts.async !== false,
        success: Frakture.DB.restSuccess,
        error: Frakture.DB.restError, _callback:callback
    });
};

/* Escape MongoDB dollar signs */
Frakture.DB.escapeDollarSign=function(o){
	if (!o) return null;
	switch (typeof o){
		case 'object':	
			for (var i in o){
				if ((typeof i=='string') && i.indexOf('$')==0){
					o['_'+i]=exports.escapeDollarSign(o[i]);
					delete o[i];
				}else{
					o[i]=exports.escapeDollarSign(o[i]);
				}
			}
		default:	return o;
	}
}

Frakture.DB.unescapeDollarSign=function(o){
	if (!o) return null;
	switch (typeof o){
		case 'object':	
			for (var i in o){
				if ((typeof i=='string') && i.indexOf('_$')==0){
					o[i.substring(1)]=exports.unescapeDollarSign(o[i]);
					delete o[i];
				}else{
					o[i]=exports.unescapeDollarSign(o[i]);
				}
			}
		default:	return o;
	}
}
