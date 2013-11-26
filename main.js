//Handles some things some as re and de-dollar signing queries
var utilities=require("frakture-utility"),
	express=require("express"),
	db = utilities.mongo.getDB(),
	async=require("async");

function listObject(req, res, next){
		var obj=req.params.object;
		var q=utilities.js.safeEval(req.param('q','{}'));
		
		try{
			if (q._id){
				 q._id=db.ObjectID.createFromHexString(q._id);
			}
		}catch(e){}
	
		if (req.param("useGlobal")=='true'){
			q.account_id={$exists:false};
		}else{
			q.account_id=req.user.current_account_id;
		}
		//Convert any $oid objects into actual BSON ids
		q=utilities.mongo.convertOid(q);
	
		var fields=req.param("fields");
		if (fields) fields=JSON.parse(fields);
		if (!fields) fields={};
		
		db.collection(obj).find(q,fields).toArray(function(err, result) {
			if (err){ next(err);return;}
			if (req.param('functions')){
				res.set('Content-Type', 'application/javascript');

				return res.send(utilities.js.serialize(result));
			}else{
				res.jsonp(result);
			}	
		});
	};
function getObject(req, res,next){

		var obj=req.params.object;
		var id=db.ObjectID.createFromHexString(req.params.id);
		var q={_id:id};
		if (req.param("useGlobal")=='true'){
			q.account_id={$exists:false};
		}else{
			q.account_id=req.user.current_account_id;
		}
		db.collection(obj).findOne(q,function(err, result) {
			if (err){ next(err);return;}
			if (req.param('functions')){
				res.set('Content-Type', 'application/javascript');

				return res.send(utilities.js.serialize(result));
			}else{
				res.jsonp(result);
			}	
		});
	}

	//Run through validation and presave functions prior to saving
	function _beforeSave(definition,data, callback){
		if (data._id && typeof data._id=='string'){
			try{
				data._id=db.ObjectID.createFromHexString(data._id);
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
		var definition=Frakture.Objects[obj] || {};
		var d=req.query.data || req.param('data');
		if (!d){ next(new Error("No parameter 'data'")); return;}
		var requestData=JSON.parse(d);
	
		var arrayRequestData=Array.isArray(requestData)?requestData:[requestData];
		var results=[];
		
		async.eachSeries(arrayRequestData,function(data,dataCallback){
			_beforeSave(definition,data,function(err,modifiedData){
				data=modifiedData;
				if (err){ dataCallback(err); return;}

				if (data.account_id && (data.account_id !=req.user.current_account_id)){next(new Error("a different account_id was specified than this users current account"));return;}
	
				data.account_id=req.user.current_account_id;
			
					//Some collections, like 'code', are incremental
					if (['code','message'].indexOf(obj)>=0 && !data._id){
						utilities.mongo.insertIncrementalDocument(data,db.collection(obj),function(err,d){
							if (err){
								results.push(null);
								 return dataCallback(err);
							}
							results.push(d._id);
							dataCallback();
						});
					}else{
						db.collection(obj).save(data,{safe:true},function(err,d){
							if (err){
								results.push(null);
								 return dataCallback(err);
							}
							results.push(d._id);
							dataCallback();
						});
					}
				}
			)},
			function(err){
				console.log(err);
				if (err){
					res.jsonp(500,err);
					res.setHeader("Error",err.toString());
					return;
				}else{
					res.jsonp({success:true,_ids:results});
				}
			}
		);
	}


	//SINGLE OR MULTIPLE UPDATE
	//If there's an ID specified, it's single, else it's multiple
function update(req, res,next){
	var obj=req.params.object;
	var definition=Frakture.Objects[obj] || {};
	var query={};
	if (req.params.id){
		 query={_id:db.ObjectID.createFromHexString(req.params.id)};
	}else{
		query=JSON.parse(req.param('q','{}'));
	}
	if ("_id" in query && !query._id) throw "_id must not be empty if specified";

	//Convert any $oid fields into actual BSON objects
	query=utilities.mongo.convertOid(query);

	var data=JSON.parse(req.param('data'));
	data=utilities.mongo.convertOid(data);

	_beforeSave(definition,data,function(errs,newData){
		if (errs){
			res.jsonp(499,err);
			res.setHeader("Error",err.toString());
			return;
		}else{
			data=newData;

			if (query.account_id){ next(new Error("account_id cannot be specified in a query"));return;}
			query.account_id=req.user.current_account_id;

			if (data.account_id){next(new Error("account_id cannot be a specified field"));return;}
			data.account_id=req.user.current_account_id;


			//Not a direct map of the 'update' command, because it's annoying to specify every field.  Thus we use '$set'
			delete data._id;

			var updateData={$set:data};

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
		 query={_id:db.ObjectID.createFromHexString(req.params.id)};
	}else{
		query=JSON.parse(req.param('q','{}'));
	}

	if (query.account_id){ next(new Error("account_id cannot be specified in a query"));return;}
	query.account_id=req.user.current_account_id;

	var obj=req.params.object;

	db.collection(obj).remove(query,{safe:true},function(err, result) {
		if (err){ next(err);return;}
		res.jsonp(result);
	});
}

exports.express=function(){
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
					}else{
						req.params.id=parts[2];
						return getObject(req,res,next);
					}
				};
				break;
			case "POST":
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

