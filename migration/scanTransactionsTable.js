const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const readline = require("readline");
const fs = require("fs");
const path = require("path");

var search = require('../../routes/search');
const { Client } = require('@opensearch-project/opensearch')
const client = new Client({ node: search.getElasticSearchEndPoint() })

var REGION = "us-east-1";
var TABLE_NAME = "com.bsafes.transactionsV2";
var INDEX_NAME = "transactionsV2TimeStamp"; // GSI: partition key gateway (String), sort key time (Number)

var useOpenBSafes = process.env.useOpenBSafes;
if (useOpenBSafes) {
  TABLE_NAME = "open." + TABLE_NAME;
}

var ddbClient = new DynamoDBClient({ region: REGION });
var dynamodbDoc = DynamoDBDocumentClient.from(ddbClient);

var gatewayArg = process.argv[2];
if (gatewayArg === undefined) {
  console.error("Missing required gateway argument.");
  console.error("Usage: node scanTransactionsTable.js <gateway> [time]");
  process.exit(1);
}
var GATEWAY_VALUE = gatewayArg;

// Optional: only consider transactions after this time.
var KEY_CONDITION = "gateway = :gw";
var EXPRESSION_VALUES = { ":gw": GATEWAY_VALUE };
var EXPRESSION_NAMES; // "time" is a reserved word in DynamoDB, so it's only aliased when actually used below

var timeStampArg = process.argv[3];
if (timeStampArg !== undefined) {
  var TIME_VALUE = Number(timeStampArg);
  if (isNaN(TIME_VALUE)) {
    console.error("Invalid time argument:", JSON.stringify(timeStampArg), "- must be a number.");
    console.error("Usage: node scanTransactionsTable.js <gateway> [time]");
    process.exit(1);
  }
  KEY_CONDITION += " AND #t > :ts";
  EXPRESSION_VALUES[":ts"] = TIME_VALUE;
  EXPRESSION_NAMES = { "#t": "time" };
  console.log("Querying for gateway:", GATEWAY_VALUE, "with time >", TIME_VALUE);
} else {
  console.log("Querying for gateway:", GATEWAY_VALUE);
}

var params = {
  TableName: TABLE_NAME,
  IndexName: INDEX_NAME,
  KeyConditionExpression: KEY_CONDITION,
  ExpressionAttributeValues: EXPRESSION_VALUES,
  ScanIndexForward: true // ascending by time
};
if (EXPRESSION_NAMES) {
  params.ExpressionAttributeNames = EXPRESSION_NAMES;
}

var number = 0;
var itemIndex = 0; // accumulated count of items seen so far, across pages/restarts

var MAX_ITEM_RETRIES = 3;
var ITEM_RETRY_DELAY_MS = 2000;
var POLL_INTERVAL_MS = 1000; // once caught up, how often to check for new items

// Where we remember the last page boundary so an aborted run can resume later.
// Keyed by gateway so runs for different gateways don't clobber each other's progress.
var SAFE_GATEWAY = GATEWAY_VALUE.replace(/[^A-Za-z0-9_-]/g, "_");
var CHECKPOINT_FILE = path.join(__dirname, "scanTransactionsTable.gateway-" + SAFE_GATEWAY + ".checkpoint.json");

function loadCheckpoint() {
  try {
    var raw = fs.readFileSync(CHECKPOINT_FILE, "utf8");
    return JSON.parse(raw);
  } catch (err) {
    return undefined;
  }
}

function saveCheckpoint(key, index) {
  try {
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify({ ExclusiveStartKey: key, itemIndex: index }));
    console.log("Checkpoint saved to", CHECKPOINT_FILE, "- ExclusiveStartKey:", JSON.stringify(key), "itemIndex:", index);
  } catch (err) {
    console.error("Warning: failed to save checkpoint:", err.message);
  }
}

function delay(ms) {
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
}

function promptToContinue(message) {
  return new Promise(function (resolve) {
    var rl = readline.createInterface({ input: process.stdin, output: process.stdout });
    rl.question(message + " (yes/no) ", function (answer) {
      rl.close();
      resolve(/^y(es)?$/i.test(String(answer).trim()));
    });
  });
}

// Read the full transaction item from the base table using the id+time from the GSI.
function getFullItem(id, time) {
  return dynamodbDoc.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: { id: id, time: time }
  })).then(function (data) {
    return data.Item;
  });
}

// Check whether this id+time is already in the 'transaction' index.
function alreadyIndexed(docId) {
  return client.exists({
    index: 'transaction',
    id: docId
  }).then(function (response) {
    return response.body === true;
  });
}

// Index the full transaction item to the 'transaction' OpenSearch index (raw item, no field transformation).
// Doc id is id+time since the base table's primary key is (id, time), not id alone.
function indexItem(transaction) {
  console.log(transaction);

  var docId = transaction.id + '-' + transaction.time;
  return client.index({
    index: 'transaction',
    id: docId,
    body: transaction
  }).catch(function (error) {
    console.log("Could not index transaction: <transaction.id>", docId);
    throw error;
  });
}

// Read the full item from the base table, then index it if not already indexed. Retries on failure.
async function processItem(gsiItem) {
  var docId = gsiItem.id + '-' + gsiItem.time;

  for (var attempt = 1; ; attempt++) {
    try {
      var exists = await alreadyIndexed(docId);
      if (exists) {
        console.log("Already indexed, skipping:", docId);
        return;
      }

      var fullItem = await getFullItem(gsiItem.id, gsiItem.time);
      if (!fullItem) {
        console.error("Base table has no item for id:", gsiItem.id, "time:", gsiItem.time, "- skipping");
        number++;
        return;
      }

      await indexItem(fullItem);
      number++;
      console.log("number:", number);
      return;
    } catch (err) {
      var label = "Processing " + docId + " failed";
      if (attempt < MAX_ITEM_RETRIES) {
        console.log(label + " (attempt " + attempt + "/" + MAX_ITEM_RETRIES + "), retrying in " + ITEM_RETRY_DELAY_MS + "ms");
        await delay(ITEM_RETRY_DELAY_MS);
        continue;
      }
      console.error(label + " after " + MAX_ITEM_RETRIES + " attempts. Error JSON:", JSON.stringify(err, null, 2));
      var retry = await promptToContinue("Retry this item? (you may raise the read capacity first) - no = skip it and continue");
      if (!retry) {
        return;
      }
      attempt = 0; // restart the retry loop
    }
  }
}

async function queryIndex() {
  if (timeStampArg !== undefined) {
    console.log("time argument provided - ignoring any saved checkpoint, starting fresh from that point.");
  } else {
    var checkpoint = loadCheckpoint();
    if (checkpoint) {
      params.ExclusiveStartKey = checkpoint.ExclusiveStartKey;
      itemIndex = checkpoint.itemIndex || 0;
      console.log("Resuming from saved checkpoint:", JSON.stringify(checkpoint.ExclusiveStartKey), "itemIndex:", itemIndex);
    }
  }

  var caughtUp = false;

  while (true) {
    var data;
    try {
      data = await dynamodbDoc.send(new QueryCommand(params));
    } catch (err) {
      console.error("Unable to query index. Error JSON:", JSON.stringify(err, null, 2));
      console.error("Count so far:", number);
      console.error("Last ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
      var retry = await promptToContinue("Retry from the last failed page? (you may raise the read capacity first)");
      if (retry) {
        console.log("Retrying from ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
        continue;
      }
      console.log("Aborted by user. Last ExclusiveStartKey:", JSON.stringify(params.ExclusiveStartKey));
      saveCheckpoint(params.ExclusiveStartKey, itemIndex);
      process.exit(1);
    }

    var items = data.Items || [];
    for (var i = 0; i < items.length; i++) {
      itemIndex++;
      console.log("[" + itemIndex + "] Processing id:", items[i].id, "time:", items[i].time);
      await processItem(items[i]);
    }

    if (items.length > 0) {
      // Remember our position: the GSI key (gateway, time) plus the
      // base table key (id) that DynamoDB requires for a GSI cursor.
      var lastItem = items[items.length - 1];
      params.ExclusiveStartKey = {
        gateway: GATEWAY_VALUE,
        time: lastItem.time,
        id: lastItem.id
      };
    }

    if (data.LastEvaluatedKey) {
      console.log("===================  Next Page (count so far: " + number + ")  ===================");
      params.ExclusiveStartKey = data.LastEvaluatedKey;
      continue;
    }

    if (!caughtUp) {
      console.log("#################### Caught up (total so far: " + number + ") ####################");
      console.log("Polling for new items every " + POLL_INTERVAL_MS + "ms. Press Ctrl+C to stop.");
      caughtUp = true;
    } else if (items.length > 0) {
      console.log("Processed " + items.length + " new item(s). total:", number);
    }

    await delay(POLL_INTERVAL_MS);
  }
}

// Ctrl+C: remember where we were so the next run can pick up here.
process.on("SIGINT", function () {
  console.log("\nInterrupted. Saving checkpoint before exiting...");
  saveCheckpoint(params.ExclusiveStartKey, itemIndex);
  process.exit(130);
});

queryIndex().catch(function (err) {
  console.error("Unexpected error:", err);
  saveCheckpoint(params.ExclusiveStartKey, itemIndex);
  process.exit(1);
});
