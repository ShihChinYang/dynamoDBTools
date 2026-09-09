var AWS = require("aws-sdk");

AWS.config.update({
    region: "us-east-1"});

var dynamodb = new AWS.DynamoDB();
var useOpenBSafes = process.env.useOpenBSafes;
var thisTable = "com.bsafes.commentsV1";
if(useOpenBSafes) {
	thisTable = "open." + thisTable;
}

var params = {
    TableName : thisTable,
    KeySchema: [
			{ AttributeName: "itemId", KeyType: "HASH"}, //Partition key
			{ AttributeName: "commentId", KeyType: "RANGE"} // Sort Key
    ],
    AttributeDefinitions: [
      { AttributeName: "itemId", AttributeType: "S" },
			{ AttributeName: "commentId", AttributeType: "S" }	
    ],
    ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1 
    }
};

dynamodb.createTable(params, function(err, data) {
    if (err) {
        console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
    } else {
        console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
    }
});


