//Handles some things some as re and de-dollar signing queries
var utilities=require("frakture-utility"),
	express=require("express"),
	workerbots=require("frakture-workerbots"),
	async=require("async");

/*
	Create a new model.  Should have:
	conn: Source data connection
	table: table name that data is stored in
	schemas: Array of possible schema sources
	
*/
function Model(options){
	for (i in options) this[i]=options[i];
}

Model.prototype.findOne=function(options,callback){
	var m=this;
	if (!callback){ callback=fields; fields=null;}
	options.table=this.table;
	m.conn.findOne(options,callback);
}
Model.prototype.find=function(options,callback){
	var m=this;
	options.table=this.table;
	m.conn.find(options,callback);
}

Model.prototype.save=function(options,callback){
	var m=this;
	options.table=this.table;
	m.conn.save(options,callback);
}


function descToJSON(desc){
	var schema=	{
	    "properties": {},
	}
	
	desc.fields.forEach(function(d){
		var o={}
		var type=d.data_type.toUpperCase();
		
		if (type.indexOf("INT")>=0) type="integer";
		else type="string";
		
		schema.properties[d.name]={
			type:type
		}
	});
	
	schema.required=desc.fields.filter(d=>d.required).map(d=>d.name);
	return schema;
}

Model.prototype.schema=function(options,callback){
	var m=this;
	m.schemas=m.schemas || [];
	//build up the schemas 
	var schema={
		"$schema": "http://json-schema.org/draft-04/schema#",
		"title": m.name,
		"type": "object",
	}
	
	console.error("Schemas:",m.schemas);
	async.eachSeries(m.schemas,function(s,scb){
		if (typeof s=='object'){
			schema=utilities.js.extend(true,schema,s);
			return scb();
		}else if (s=="describe"){
			m.conn.describe({table:m.table},function(e,desc){
				if (e) return scb(e);
				schema=utilities.js.extend(true,schema,descToJSON(desc));
				return scb();
			});
		}else{
			return scb("Unsupported schema:"+s);
		}
	
	},function(e){
		if (e) return callback(e);
		return callback(null,schema);
	});
}


var ORM=function(_config){
	if (!_config) throw "No connection configuration specified";
	var instanceConfig=_config;

	function getConnection(config,callback){
		if (config.conn) return callback(null,config.conn);
		if (!config.type) return callback("You must specify a connection type");
	
		var constructor=null;
		switch(config.type){
			case "mongodb":
				constructor=workerbots("MongoBot"); break;
			case "mysql":
				constructor=workerbots("MySQLBot"); break;
			case "sqlserver":
				constructor=workerbots("MSSBot"); break;
			default:return callback("Connection type "+config.type+" not supported");
		}
	
		var connector=new constructor({auth:config.authentication,account_id:"dev"});
		return callback(null,connector);
	}


	var models={}
	/*
		Gets a model object from a connection (default root) and name
		{
		  name: <required>
		  connection: optional namespace/connection prefix
		}
	*/
	function getModel(options,callback){
		if (!options.name) return callback("name is required");
	
		options.connection=options.connection || "root";
		
		
		var id=options.name;
		if (models[id]) return callback(null,models[id]);

		getConnection(instanceConfig,function(e,conn){
			if (e) return callback(e);
			var name=options.name;
		
			var modelConfig=instanceConfig.objects[options.name];
			if (!modelConfig){
				if (instanceConfig.objects["*"]){
					modelConfig=instanceConfig.objects["*"];
					if (typeof modelConfig!='object') modelConfig={};
					 modelConfig.table=options.name;
				}
				
				else return callback(404,"Could not find object definition for "+options.name);
			}
			
			var filters=[].concat(instanceConfig.filters).concat(modelConfig.filters).filter(Boolean);
			
			var schemas=[].concat(instanceConfig.schemas).concat(modelConfig.schemas).filter(Boolean);

			var m=new Model({
				conn:conn,
				name:options.name,
				table:modelConfig.table || options.name,
				schemas:schemas,
				filters:filters,
				primary_key:modelConfig.primary_key
			});
		
			models[id]=m;
			return callback(null,m);
		});
	}

	function getFilters(req,filters){
		var filter={};

		filters.forEach(function(f){
			if (!f) return;
			if (typeof f=='string') f=JSON.parse(f);
			if (typeof f=='function') f=f(req);
			if (typeof f!='object') throw "Invalid filter: "+f;
			utilities.js.extend(true,filter,f);
		});
		
		return filter;
	}
	

	function listObject(req, res, next){

		var fields=req.query.fields;
		try{
			if (fields) fields=JSON.parse(fields);
		}catch(e){
			return res.jsonp(400,"Invalid fields JSON");
		}
		if (fields && fields.max) return res.jsonp(400,"Fields projection must not have a field named 'max' -- see MongoDB");
	
		if (!fields) fields={};
	
		var opts={
			fields:fields	
		}
	
		if (req.query.sort){
			opts.sort=JSON.parse(req.query.sort);
		}
	
		var limit=req.query.limit;
		if (limit==parseInt(limit)){
			if (limit>500) limit=500;
			opts.limit=parseInt(limit);
		}
	
		var skip=req.query.offset;
		if (skip==parseInt(skip)){
			opts.skip=parseInt(skip);
		}
	
		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			var q=(req.body||{}).q || req.query.q;

			var filters=[q].concat(m.filters);
			opts.filter=getFilters(req,filters);

			m.find(opts,function(err, result) {
				utilities.js.timer("completed find");
				if (err){ console.error(opts); next(err);return;}
				var r=result.results;
				res.jsonp(r);
			});
		});
	}


	function schema(req,res,next){
		var obj=req.params.object;
	
		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			var opts={}
			
			m.schema(opts,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				res.set('Content-Type', 'application/javascript');
				return res.send(utilities.js.serialize(result));
			});
		});
	}

	
	function count(req,res,next){
		var obj=req.params.object;
	
		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			var opts={}
			opts.filter=getQuery(req,m.filters);
			m.count(opts,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				if (req.query.functions){
					res.set('Content-Type', 'application/javascript');
					return res.send(utilities.js.serialize(result));
				}else{
					res.jsonp(result);
				}	
			});
		});
	}

	function getObject(req, res,next){
		var obj=req.params.object;
		
		
		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			
			var id=req.params.id;
			id=utilities.mongo.getObjectID(req.params.id,true);
			if (parseInt(id)==id) id=parseInt(id);
			m.primary_key=m.primary_key||"_id";
			var filter={};
			
			filter[m.primary_key]=id;
			
			var filters=[filter].concat(m.filters);
			var opts={};
			
			opts.filter=getFilters(req,filters);
			
			m.findOne(opts,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				if (req.query.functions){
					res.set('Content-Type', 'application/javascript');
					return res.send(utilities.js.serialize(result));
				}else{
					res.jsonp(result);
				}	
			});
		});
	}

	//Run through validation and presave functions prior to saving
	function _beforeSave(definition,data, callback){
		if (data._id && typeof data._id=='string'){
			try{
				data._id=utilities.mongo.getObjectID(data._id);
			}catch(e){
			}
		}
		var errs=[];
		for (i in definition.validation){
			var func=definition.validation[i];
			if (typeof func!='function'){
				 next(new Error("Invalid object definition, bad validation function:"+obj)); return;
			}
			try{
				func(data);
			}catch(err){
				if (err.length){errs=errs.concat(err);}
				else errs.push(err);
			}
		}

		if (errs.length>0){
			callback(errs);
		}else{
			callback(null,data);
		}
	}

	//CREATE
	function save(req,res, next){
		var obj=req.params.object;
		var definition=models[obj] || {};
		var d=(req.query||{}).data || (req.body||{}).data;
		
		if (!d){ next(new Error("No parameter 'data' found in "+JSON.stringify(req.body))); return;}
		var requestData=JSON.parse(d);
		
		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			var arrayRequestData=Array.isArray(requestData)?requestData:[requestData];
			var results=[];
		
			async.eachSeries(arrayRequestData,function(data,dataCallback){
				_beforeSave(definition,data,function(err,modifiedData){
				
					data=modifiedData;
					if (err){ dataCallback(err); return;}
				
				
					/*
					//Need to add this back in with filters at some point
					if (data.account_id && (data.account_id !=req.user.current_account_id)){next(new Error("a different account_id was specified than this users current account"));return;}
	
					data.account_id=req.user.current_account_id;
					code and message are also incremental, so standard save won't work
					*/
						m.save({data:data},function(err, result) {
							if (err){
									results.push(null);
									 return dataCallback(err);
								}
								results.push(result._id);
								dataCallback();
						});
					}
				)},
				function(err){
					if (err){
						console.log(err);
						res.jsonp(500,err);
						res.setHeader("Error",err.toString());
						return;
					}else{
						res.jsonp({success:true,_ids:results});
					}
				}
			);
		});
	}


	//SINGLE OR MULTIPLE UPDATE
	//If there's an ID specified, it's single, else it's multiple
	function update(req, res,next){
		var obj=req.params.object;
		var definition=models[obj] || {};
		var query={};
		if (req.params.id){
			var val=parseInt(req.params.id);
			if (val!=req.params.id){
				val=utilities.mongo.getObjectID(req.params.id,true)
			}
			 query={_id:val};
		}else{
			query=JSON.parse(req.body.q);
		}
		
		if ("_id" in query && !query._id){
			res.jsonp(499,"_id must not be empty if specified");
			res.setHeader("Error","_id must not be empty if specified");
			return;
		}

		//Convert any $oid fields into actual BSON objects
		query=utilities.mongo.convertOid(query);

		var data=JSON.parse(req.body.data || req.query.data);
		data=utilities.mongo.convertOid(data);
		//convert any $date objects to real dates
		data=utilities.mongo.convertDate(data);
	

		_beforeSave(definition,data,function(errs,newData){
			if (errs){
				res.jsonp(499,errs);
				res.setHeader("Error",err.toString());
				return;
			}else{
				data=newData;

				if (query.account_id){ next(new Error("account_id cannot be specified in a query"));return;}
				/*
				TODO
				query.account_id=req.user.current_account_id;
				*/

				if (data.account_id){next(new Error("account_id cannot be a specified field"));return;}

				//Log a warning if there are not update fields, and assume set
				delete data._id;
				var hasDollarField=false;
				for (i in data){
					if (i.indexOf('$')==0){ hasDollarField=true; break;}
				}
				updateData=data;
				if (!hasDollarField){
					console.error("Warning, a DB update call for collection "+obj+" does not specify any update fields, assuming $set.  Referer="+req.headers.referer);
					updateData={$set:data};
				}


				db.collection(obj).findAndModify(query,[['_id','asc']],updateData,{safe:true,"new":true,upsert:true},function(err, result) {
					if (err){
						res.jsonp(500,err);
						res.setHeader("Error",err.toString());
						return;
					}
	
					res.jsonp(result);
				});
			}
		});
	}

	function deleteObjects(req,res,next){
		var query={};
		if (req.params.id){
			try{
				//some tables do not user hex strings for ids
				 query={_id:utilities.mongo.getObjectID(req.params.id)};
			}catch(e){
				if (parseInt(req.params.id)==req.params.id) query={_id:parseInt(req.params.id)};
				else query={_id:req.params.id};
			}
		}else{
			query=JSON.parse(req.body.q || req.query.q);
		}
	
		if (query.account_id){ next(new Error("account_id cannot be specified in a query"));return;}
		//TODO
		//query.account_id=req.user.current_account_id;
	
		var obj=req.params.object;

		db.collection(obj).remove(query,{safe:true},function(err, result) {
			if (err){ next(err);return;}
			res.jsonp(result);
		});
	}

	this.express=function(){

		return function(req,res,next){
	
			var parts=req.url.split("?")[0].split("/");
			if (parts[1]=="js") return express.static(__dirname)(req,res,next);
			req.params={};
			req.params.object=parts[1];

			switch(req.method){
				case "GET":
					if (parts.length==2) return listObject(req,res,next);
					if (parts[2]){
						if (parts[2]=='_save'){
							//Because of the cross-domain restrictions that are not QUITE overcome by CORS in mobile browsers, 
							// we do allow a '_save' GET request, (instead of a POST)
							return save(req,res,next);
						}else if (parts[2]=='count'){
							return count(req,res,next);
						}else if (parts[2]=='schema'){
							return schema(req,res,next);
						}else{
							req.params.id=parts[2];
							return getObject(req,res,next);
						}
					};
					break;
				case "POST":
					//Because of unfortunate limitations on the size of a GET query, allow for POST requests to /list
					if (parts[2]=="list") return listObject(req,res,next);
			
					return save(req,res,next);
					break;
		
				case "PUT":  //   /:object/:id
					if (parts[2]){req.params.id=parts[2];}
					return update(req,res,next);
	

				//DELETE
				case "DELETE":
					if (parts[2]){req.params.id=parts[2];}
					return deleteObjects(req,res,next);
					break;
				}
				next();
		 }
	}
}
module.exports=ORM;