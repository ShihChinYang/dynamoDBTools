const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, ScanCommand } = require("@aws-sdk/lib-dynamodb");
const readline = require("readline");

var search = require('../routes/search');
const { Client } = require('@opensearch-project/opensearch')
const client = new Client({ node: search.getElasticSearchEndPoint() })

var ddbClient = new DynamoDBClient({ region: "us-east-1" });

var dynamodbDoc = DynamoDBDocumentClient.from(ddbClient);
var tableName = "com.bsafes.itemVersionsV2";
var useOpenBSafes = process.env.useOpenBSafes;
if(useOpenBSafes) {
  tableName = "open." + tableName;
}

var params = {
  TableName : tableName
};

var number = 0;

var MAX_INDEX_RETRIES = 3;
var INDEX_RETRY_DELAY_MS = 2000;

function promptToContinue(message, cb) {
  var rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  rl.question(message + " (yes/no) ", function (answer) {
    rl.close();
    cb(/^y(es)?$/i.test(String(answer).trim()));
  });
}

function indexItem(item, done) {

	var updatedBy = item.updatedBy?item.updatedBy:item.owner;
	var displayName = item.displayName?item.displayName:updatedBy;
	var indexBody = {
		id: item.id,
		version: item.version,
		space: item.space,
		createdTime: item.createdTime,
		displayName: displayName,
		updatedBy: updatedBy,
		update: item.update?item.update:"creation",
		type: item.type,
		keyEnvelope: item.keyEnvelope,
		envelopeIV: item.envelopeIV,
    ivEnvelope: item.ivEnvelope,
    ivEnvelopeIV: item.ivEnvelopeIV,
    type: item.type,
    title: item.title
  };

  console.log(indexBody);

	var indexId = item.id + '-' + item.version;
  client.index({
    index: 'itemversion',
    id: indexId,
    body: indexBody
  }).then(function (response) {
		number ++;
		console.log("number:", number);
		done(null);
  }).catch(function (error) {
		number ++;
		console.log("number:", number);
		console.log("Could not index item: <itemversion.indexId>", indexId);
		done(error);
  });
}

function scanTable() {
	dynamodbDoc.send(new ScanCommand(params)).then(function(data) {
    {
      var i = 0;

      function advance() {
        i ++;
        if(i < data.Items.length) {
          indexAnItem(data.Items[i], 1);
        } else {
          requestForNextPage();
        }
      }

      function indexAnItem(item, attempt) {
        attempt = attempt || 1;
        indexItem(item, function(err) {
          if(!err) {
            advance();
            return;
          }

          if(attempt < MAX_INDEX_RETRIES) {
            console.log("Index failed (attempt " + attempt + "/" + MAX_INDEX_RETRIES + "), retrying in " + INDEX_RETRY_DELAY_MS + "ms");
            setTimeout(function() {
              indexAnItem(item, attempt + 1);
            }, INDEX_RETRY_DELAY_MS);
            return;
          }

          console.error("Index still failing after " + MAX_INDEX_RETRIES + " attempts. Error JSON:", JSON.stringify(err, null, 2));
          promptToContinue("Skip this item and continue?", function(yes) {
            if(yes) {
              advance();
            } else {
              console.log("Aborted by user. Last ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
              process.exit(1);
            }
          });
        });
      };


      if(data.Items.length) {
        indexAnItem(data.Items[i], 1);
      }

      function requestForNextPage() {
        if(data.LastEvaluatedKey) {
          console.log("number: ", number);
          console.log("===================  Next Page ===================");
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          scanTable();
        } else {
          console.log("#################### Done ########################");
          console.log("total: ", number);
        }
      }
    }
	}).catch(function(err) {
      console.error("Unable to scan table. Error JSON:", JSON.stringify(err, null, 2));
      promptToContinue("Continue from the last failed page?", function(yes) {
        if(yes) {
          console.log("Retrying from last ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
          scanTable();
        } else {
          console.log("Aborted by user. Last ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
          process.exit(1);
        }
      });
	});
}

scanTable();
