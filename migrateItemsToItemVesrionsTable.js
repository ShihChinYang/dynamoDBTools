var AWS = require("aws-sdk");

AWS.config.update({
    region: "us-east-1"});

var dynamodbDoc = new AWS.DynamoDB.DocumentClient();

var params = {
    TableName : "com.bsafes.items"
};

function copyingItem(item, done) {
	console.log(item);
	console.log('-------------------------------------------------');
	var thisTable = "com.bsafes.itemVersions";

	item.version = 1;
	var params = {
    TableName: thisTable,
    Item: item 
  };

	dynamodbDoc.put(params, function(err, data) {
    if(err) {
      console.error("Unable to add itemVersion");
        done(err);
      } else {
        console.log("PutItem succeeded:");
       	done(null);
      }
    });
};

var number = 0;
function scanTable() {
  dynamodbDoc.scan(params, function(err, data) {
    if (err) {
      console.error("Unable to scan table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
			var index = 0;

			function copyItem() {
				var item = data.Items[index];
				copyingItem(item, function(err) {
					if(err) {
						console.log(err);
					} else {
						index ++;
						number ++;
						if(index < data.Items.length) {
							copyItem();
						} else {
      				if(data.LastEvaluatedKey) {
        				console.log("===================  Next Page ===================");
        				params.ExclusiveStartKey = data.LastEvaluatedKey;
        				scanTable();
      				} else {
        				console.log("#################### Done ########################");
        				console.log("total: ", number);
     	 				}
						}
					}
				});	
			}
			if(data.Items.length) {
				copyItem();
			}
    }
  });
}

scanTable();
