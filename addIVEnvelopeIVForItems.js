var AWS = require("aws-sdk");

var search = require('../routes/search');
var elasticsearch = require('elasticsearch');
var eSClient = new elasticsearch.Client({
  host: search.getElasticSearchEndPoint(),
  log: 'trace'
});

AWS.config.update({
    region: "us-east-1"});

var dynamodb = new AWS.DynamoDB();
var dynamodbDoc = new AWS.DynamoDB.DocumentClient();

var params = {
    TableName : "com.bsafes.items"
};

function updateItem(item, done) {
	var itemId = item.id.S;
	var ivEnvelopeIV = item.envelopeIV.S;

  var params = {
    TableName : "com.bsafes.items",
    Key: {"id" : itemId},
    "UpdateExpression" : "SET ivEnvelopeIV = :ivEnvelopeIV",
    ExpressionAttributeValues: {
      ":ivEnvelopeIV": ivEnvelopeIV 
    }
  };

  console.log("Updating ivEnvelopeIV for an item ...");

  dynamodbDoc.update(params, function(err, data) {
    if (err) {
      console.error("Unable to update ivEnvelopeIV. Error JSON:", JSON.stringify(err, null, 2));
      done(err, null);
    } else {
      console.log("Updating ivEnvelopeIV:", JSON.stringify(data, null, 2));
			
			eSClient.update({
    		index: 'item',
    		type: 'item',
    		id: itemId,
    		body: {
      		doc:{
						ivEnvelopeIV: ivEnvelopeIV
      		}
    		}
  		}, function (error, response) {
				if(error) {
					done(error);
				} else {
					done(null);
				}
  		});
    }
  });
};

var number = 0;
function scanTable() {
	dynamodb.scan(params, function(err, data) {
    if (err) {
      console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
/*
			data.Items.forEach(function(element, index, array){
				console.log(element);
				number ++;
				updateItem(element);
				console.log('-------------------------------------------------');
			});
*/

			if(data.Items.length) {
				var index = 0;
				function itemHandler() {
					var item = data.Items[index];
					number ++;
					console.log(item);
					updateItem(item, function(err){
						if(err) {
							console.log(err);
						} else {
							index ++;
							if(index < data.Items.length) {
								itemHandler();
							}
						}
					});			
				}
			
				itemHandler();
			}

			if(data.LastEvaluatedKey) {
				console.log("===================  Next Page ===================");
				params.ExclusiveStartKey = data.LastEvaluatedKey;
				scanTable();
			} else {
				console.log("#################### Done ########################");
				console.log("total: ", number);
			}
    }
	});
}

scanTable();
