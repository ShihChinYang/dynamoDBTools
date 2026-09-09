const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, ScanCommand, QueryCommand } = require("@aws-sdk/lib-dynamodb");
const readline = require("readline");

var search = require('../routes/search');
const { Client } = require('@opensearch-project/opensearch')
const client = new Client({ node: search.getElasticSearchEndPoint() })

var ddbClient = new DynamoDBClient({ region: "us-east-1" });

var dynamodbDoc = DynamoDBDocumentClient.from(ddbClient);
var tableName = "com.bsafes.itemVersionsV2";
var useOpenBSafes = process.env.useOpenBSafes;
if (useOpenBSafes) {
	tableName = "open." + tableName;
}

var params = {
	TableName: tableName
};

var number = 0;

function promptToContinue(message, cb) {
	var rl = readline.createInterface({ input: process.stdin, output: process.stdout });
	rl.question(message + " (yes/no) ", function (answer) {
		rl.close();
		cb(/^y(es)?$/i.test(String(answer).trim()));
	});
}

var MAX_INDEX_RETRIES = 3;
var INDEX_RETRY_DELAY_MS = 2000;

function indexItem(thisItem, done) {
	var itemId = thisItem.id;

	function buildIndexBody(item) {
		var itemPath = [];
		if (item.path) {
			item.path.forEach(function (container) {
				itemPath.push(container);
			});
		}
		var itemTitleTokens = [];
		if (item.titleTokens) {
			itemTitleTokens = item.titleTokens;
		} else if (item.titleTokens && item.titleTokens) {
			item.titleTokens.forEach(function (titleToken) {
				itemTitleTokens.push(titleToken);
			})
		}
		var itemTags = [];
		if (item.tags) {
			item.tags.forEach(function (tag) {
				itemTags.push(tag);
			});
		};
		var itemTagsTokens = [];
		if (item.tagsTokens) {
			item.tagsTokens.forEach(function (tagToken) {
				itemTagsTokens.push(tagToken);
			});
		}

		var indexBody = {
			keyVersion: item.keyVersion,
			keyEnvelope: item.keyEnvelope,
			ivEnvelope: item.ivEnvelope,
			envelopeIV: item.envelopeIV,
			ivEnvelopeIV: item.ivEnvelopeIV,
			space: item.space,
			container: item.container,
			path: itemPath,
			position: item.position,
			totalItemVersions: item.totalItemVersions,
			totalStorage: item.totalStorage,
			titleTokens: itemTitleTokens,
			tags: itemTags,
			tagsTokens: itemTagsTokens,
			type: item.type,
			version: item.version
		};

		if (item.totalItemVersions !== undefined) {
			indexBody.totalItemVersions = item.totalItemVersions;
			indexBody.totalStorage = item.totalStorage;
		}

		if (item.usage) {
			indexBody.size = item.usage.dbSize;
			indexBody.totalItemSize = item.usage.totalItemSize;
		}

		if (item.title && item.title) indexBody.title = item.title;

		if (item.pageNumber) indexBody.pageNumber = item.pageNumber;
		if (item.recycled) indexBody.recycled = item.recycled;

		console.log(indexBody);
		return indexBody;
	}

	console.log(itemId);

	function checkIsIndexed(thisItemId, checkCB) {
		client.get({
			index: 'item',
			id: thisItemId
		}).then(function (resp) {
			//console.log(JSON.stringify(resp));
			if (resp.body.found) {
				console.log("Already indexed");
				checkCB(null, true);
			} else {
				checkCB(null, false);
			}
		}).catch(function (err) {
			// A missing document rejects with a 404; treat that as "not indexed".
			if (err && err.meta && err.meta.statusCode === 404) {
				checkCB(null, false);
			} else {
				console.log(JSON.stringify(err));
				checkCB(err);
			}
		});
	}

	function indexLatestVersion(itemId, indexCB) {
		var params = {
			TableName: tableName,
			KeyConditionExpression: "#id = :id",
			ExpressionAttributeNames: {
				"#id": "id",
			},
			ExpressionAttributeValues: {
				":id": itemId,
			},
			ScanIndexForward: false
		};

		dynamodbDoc.send(new QueryCommand(params)).then(function (data) {
			{
				if (data.Items.length) {
					var thisItem = data.Items[0];
					//console.log("************************* :", JSON.stringify(thisItem));
					var indexBody = buildIndexBody(thisItem);

					client.index({
						index: 'item',
						refresh: true,
						id: itemId,
						body: indexBody
					}).then(function (response) {
						number++;
						console.log("number:", number);
						console.log("********** Indexed");
						indexCB(null);
					}).catch(function (error) {
						number++;
						console.log("number:", number);
						console.log("Could not index item: <item.id>", itemId);
						indexCB(error);
					});
				} else {
					indexCB("No items found.");
				}
			}
		}).catch(function (err) {
			indexCB(err);
		});
	}

	checkIsIndexed(itemId, function (err, found) {
		if (err) {
			indexLatestVersion(itemId, function (err) {
				done(err);
			});
		} else {
			if (!found) {
				indexLatestVersion(itemId, function (err) {
					done(err);
				});
			} else {
				done(null);
			}
		}
	});
}

function scanTable() {
	dynamodbDoc.send(new ScanCommand(params)).then(function (data) {
		{
			var i = 0;

			function advance() {
				i++;
				if (i < data.Items.length) {
					indexAnItem(data.Items[i], 1);
				} else {
					requestForNextPage();
				}
			}

			function indexAnItem(item, attempt) {
				attempt = attempt || 1;
				indexItem(item, function (err) {
					if (!err) {
						advance();
						return;
					}

					if (attempt < MAX_INDEX_RETRIES) {
						console.log("Index failed for item " + item.id + " (attempt " + attempt + "/" + MAX_INDEX_RETRIES + "), retrying in " + INDEX_RETRY_DELAY_MS + "ms");
						setTimeout(function () {
							indexAnItem(item, attempt + 1);
						}, INDEX_RETRY_DELAY_MS);
						return;
					}

					console.error("Index still failing after " + MAX_INDEX_RETRIES + " attempts for item " + item.id + ". Error JSON:", JSON.stringify(err, null, 2));
					promptToContinue("Skip this item and continue?", function (yes) {
						if (yes) {
							advance();
						} else {
							console.log("Aborted by user at item:", item.id);
							console.log("Last ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
							process.exit(1);
						}
					});
				});
			};


			if (data.Items.length) {
				indexAnItem(data.Items[i], 1);
			}

			function requestForNextPage() {
				if (data.LastEvaluatedKey) {
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
	}).catch(function (err) {
		console.error("Unable to scan table. Error JSON:", JSON.stringify(err, null, 2));
		promptToContinue("Continue from the last failed page?", function (yes) {
			if (yes) {
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
