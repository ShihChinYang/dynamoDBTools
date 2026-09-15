const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const readline = require("readline");
const fs = require("fs");
const path = require("path");

var search = require('../../routes/search');
const { Client } = require('@opensearch-project/opensearch')
const client = new Client({ node: search.getElasticSearchEndPoint() })

var REGION = "us-east-1";
var TABLE_NAME = "com.bsafes.itemVersionsV2";
var INDEX_NAME = "itemVersionsV2TimeStamp"; // GSI: partition key keyVersion, sort key createdTime

var useOpenBSafes = process.env.useOpenBSafes;
if (useOpenBSafes) {
  TABLE_NAME = "open." + TABLE_NAME;
}

var ddbClient = new DynamoDBClient({ region: REGION });
var dynamodbDoc = DynamoDBDocumentClient.from(ddbClient);

var keyVersionArg = process.argv[2];
if (keyVersionArg === undefined) {
  console.error("Missing required keyVersion argument.");
  console.error("Usage: node scanItemVersionsIndex.js <keyVersion> [timeStamp]");
  process.exit(1);
}
var KEY_VERSION_VALUE = Number(keyVersionArg);
if (!Number.isInteger(KEY_VERSION_VALUE)) {
  console.error("Invalid keyVersion argument:", JSON.stringify(keyVersionArg), "- must be an integer.");
  console.error("Usage: node scanItemVersionsIndex.js <keyVersion> [timeStamp]");
  process.exit(1);
}

// Optional: only consider items created after this createdTime.
var KEY_CONDITION = "keyVersion = :kv";
var EXPRESSION_VALUES = { ":kv": KEY_VERSION_VALUE };

var timeStampArg = process.argv[3];
if (timeStampArg !== undefined) {
  var asNumber = Number(timeStampArg);
  var TIMESTAMP_VALUE = isNaN(asNumber) ? timeStampArg : asNumber;
  KEY_CONDITION += " AND createdTime > :ts";
  EXPRESSION_VALUES[":ts"] = TIMESTAMP_VALUE;
  console.log("Querying for keyVersion:", KEY_VERSION_VALUE, "with createdTime >", TIMESTAMP_VALUE);
} else {
  console.log("Querying for keyVersion:", KEY_VERSION_VALUE);
}

var params = {
  TableName: TABLE_NAME,
  IndexName: INDEX_NAME,
  KeyConditionExpression: KEY_CONDITION,
  ExpressionAttributeValues: EXPRESSION_VALUES,
  ScanIndexForward: true // ascending by createdTime
};

var number = 0;
var itemIndex = 0; // accumulated count of items seen so far, across pages/restarts

var MAX_ITEM_RETRIES = 3;
var ITEM_RETRY_DELAY_MS = 2000;
var POLL_INTERVAL_MS = 1000; // once caught up, how often to check for new items

// Where we remember the last page boundary so an aborted run can resume later.
// Keyed by keyVersion so runs for different keyVersions don't clobber each other's progress.
var CHECKPOINT_FILE = path.join(__dirname, "scanItemVersionsIndex.keyVersion-" + KEY_VERSION_VALUE + ".checkpoint.json");

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

// Read the full item from the base table using the id (+ version) from the GSI.
function getFullItem(id, version) {
  return dynamodbDoc.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: { id: id, version: version }
  })).then(function (data) {
    return data.Item;
  });
}

// Check whether this id/version is already in the 'itemversion' index.
function alreadyIndexed(id, version) {
  var indexId = id + '-' + version;
  return client.exists({
    index: 'itemversion',
    id: indexId
  }).then(function (response) {
    return response.body === true;
  });
}

// Index the full item to the 'itemversion' OpenSearch index (same shape as scanItemVersionIndex.js).
function indexItem(item) {
  var updatedBy = item.updatedBy ? item.updatedBy : item.owner;
  var displayName = item.displayName ? item.displayName : updatedBy;
  var indexBody = {
    id: item.id,
    version: item.version,
    space: item.space,
    container: item.container,
    createdTime: item.createdTime,
    displayName: displayName,
    updatedBy: updatedBy,
    update: item.update ? item.update : "creation",
    type: item.type,
    keyEnvelope: item.keyEnvelope,
    title: item.title
  };

  if (item.envelopeIV) indexBody.envelopeIV = item.envelopeIV;
  if (item.ivEnvelope) indexBody.ivEnvelope = item.ivEnvelope;
  if (item.ivEnvelopeIV) indexBody.ivEnvelopeIV = item.ivEnvelopeIV;

  if (item.usage) {
    indexBody.size = item.usage.dbSize;
    indexBody.totalItemSize = item.usage.totalItemSize;
  }
  console.log(indexBody);

  var indexId = item.id + '-' + item.version;
  return client.index({
    index: 'itemversion',
    id: indexId,
    body: indexBody
  }).catch(function (error) {
    console.log("Could not index item: <itemversion.indexId>", indexId);
    throw error;
  });
}

// Read the full item from the base table, then index it. Retries on failure.
async function processItem(gsiItem) {
  for (var attempt = 1; ; attempt++) {
    try {
      var exists = await alreadyIndexed(gsiItem.id, gsiItem.version);
      if (exists) {
        console.log("Already indexed, skipping:", gsiItem.id + '-' + gsiItem.version);
        return;
      }

      var fullItem = await getFullItem(gsiItem.id, gsiItem.version);
      if (!fullItem) {
        console.error("Base table has no item for id:", gsiItem.id, "version:", gsiItem.version, "- skipping");
        number++;
        return;
      }

      await indexItem(fullItem);
      number++;
      console.log("number:", number);
      return;
    } catch (err) {
      var label = "Processing " + gsiItem.id + " v" + gsiItem.version + " failed";
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
    console.log("timeStamp argument provided - ignoring any saved checkpoint, starting fresh from that point.");
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
      console.log("[" + itemIndex + "] Processing id:", items[i].id, "version:", items[i].version);
      await processItem(items[i]);
    }

    if (items.length > 0) {
      // Remember our position: the GSI key (keyVersion, createdTime) plus the
      // base table key (id, version) that DynamoDB requires for a GSI cursor.
      var lastItem = items[items.length - 1];
      params.ExclusiveStartKey = {
        keyVersion: KEY_VERSION_VALUE,
        createdTime: lastItem.createdTime,
        id: lastItem.id,
        version: lastItem.version
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
