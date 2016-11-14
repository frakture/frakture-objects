//Handles some things some as re and de-dollar signing queries
var utilities=require("frakture-utility"),
	express=require("express"),
	workerbots=require("frakture-workerbots"),
	async=require("async");
	
function debug(){
	console.error.apply(this,arguments);
}

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

Model.prototype.insert=function(options,callback){
	var m=this;
	options.table=this.table;
	m.conn.insert(options,callback);
}

Model.prototype.update=function(options,callback){
	var m=this;
	options.table=this.table;
	m.conn.update(options,callback);
}

Model.prototype.remove=function(options,callback){
	var m=this;
	options.table=this.table;
	m.conn.remove(options,callback);
}


Model.prototype.tag=function(options,callback){
	var m=this;
	options.table=this.table;
	m.conn.tag(options,callback);
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
			var beforeSave=[].concat(instanceConfig.beforeSave).concat(modelConfig.beforeSave).filter(Boolean);
			
			var schemas=[].concat(instanceConfig.schemas).concat(modelConfig.schemas).filter(Boolean);

			var m=new Model({
				conn:conn,
				name:options.name,
				table:modelConfig.table || options.name,
				schemas:schemas,
				filters:filters,
				beforeSave:beforeSave,
				primary_key:modelConfig.primary_key || "id"
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
			
			console.error(opts);

			m.find(opts,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				var r=result.results;
				if(!r){
					console.error(result);
					return res.jsonp(500,"Error getting results");
				}
				r.forEach(function(d){
					if (d._id){
						d.id=d._id;
						delete d._id;
					}
					
				});
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
	
	function tag(req,res,next){
		var obj=req.params.object;
	
		getModel({name:obj, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			var o=utilities.js.extend({},req.query);
			o.object=obj;
			o.id=req.params.id;

			m.tag(o,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				res.jsonp(result);
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
			
			m.primary_key=m.primary_key||"id";
			var filter={};
			
			filter[m.primary_key]=id;
			
			var filters=[filter].concat(m.filters);
			var opts={};
			
			opts.filter=getFilters(req,filters);
			
			m.findOne(opts,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				if (!result) return res.jsonp(404,{error:"Not found",filter:opts});
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
	function _beforeSave(req,model,id,name,data, options, callback){
		
		var errs=[];
		
		async.eachSeries(model.beforeSave,function(f,fcb){
			f({name:name,request:req,id:id,data:data},fcb);
		},function(e){
			if (e) return callback(e);
		
			var errs=[];
			for (i in model.validation){
				var func=model.validation[i];
				if (typeof func!='function'){
					 next(new Error("Invalid object model, bad validation function:"+obj)); return;
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
		
		});
		
		
	}

	//SINGLE RECORD UPSERT
	//This does NOT handle bulk updates right now

	function upsert(req, res,next){
		var obj=req.params.object;
		
		var id=req.params.id;
		
		var data=JSON.parse(req.body.data || req.query.data);
		//Legacy support for $set, which is deprecated for an upsert
		if (data.$set) data=data.$set;
		

		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}

			_beforeSave(req,m,id,m.name,data, {},function(errs){
				if (!data.account_id) return res.jsonp(500,"No error specified");

				if (errs){
					res.jsonp(499,errs);
					res.setHeader("Error",err.toString());
					return;
				}else{

					//ID has already been specified, don't duplicate it
					delete data[m.primary_key];
					//Log a warning if there are not update fields, and assume set
					
					if (id){
						m.update({id:id,data:data},function(err,result){
							if (err){
								debug(err);
								res.jsonp(500,err);
								res.setHeader("Error",err.toString());
								return;
							}
	
							res.jsonp(result.data);
						});
					}else{
						m.insert({data:data},function(err,result){
							if (err){
								debug(err);
								res.jsonp(500,err);
								res.setHeader("Error",err.toString());
								return;
							}
	
							res.jsonp(result.data);
						});
					}
				}
			});
		});
	}
	
	function deleteObjects(req, res,next){
		var obj=req.params.object;
		getModel({name:req.params.object, connection:req.params.connection},function(e,m){
			if (e){
				if (parseInt(e)==e) return res.jsonp(e,m);
				return res.jsonp(500,e);
			}
			
			m.primary_key=m.primary_key||"id";
			
			var filter={};
			if (req.params.id){
				filter[m.primary_key]=req.params.id;
				
			}else{
				return next("multiple delete not currently supported");
			}
			
			var filters=[filter].concat(m.filters);
			var opts={};
			
			opts.filter=getFilters(req,filters);
			
			m.remove(opts,function(err, result) {
				if (err){ console.error(opts); next(err);return;}
				res.jsonp(result);
			});
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
							if (parts[3]=="tag"){return tag(req,res,next);}
							return getObject(req,res,next);
						}
					};
					break;
				case "POST":
					//Because of unfortunate limitations on the size of a GET query, allow for POST requests to /list
					if (parts[2]=="list") return listObject(req,res,next);
			
					return upsert(req,res,next);
					break;
		
				case "PUT":  //   /:object/:id
					if (parts[2]){req.params.id=parts[2];}
					if (parts[3]=="tag"){return tag(req,res,next);}
					
					return upsert(req,res,next);
					
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