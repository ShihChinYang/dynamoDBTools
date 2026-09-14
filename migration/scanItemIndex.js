const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");
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
  console.error("Usage: node scanItemIndex.js <keyVersion> [timeStamp]");
  process.exit(1);
}
var KEY_VERSION_VALUE = Number(keyVersionArg);
if (!Number.isInteger(KEY_VERSION_VALUE)) {
  console.error("Invalid keyVersion argument:", JSON.stringify(keyVersionArg), "- must be an integer.");
  console.error("Usage: node scanItemIndex.js <keyVersion> [timeStamp]");
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
var CHECKPOINT_FILE = path.join(__dirname, "scanItemIndex.keyVersion-" + KEY_VERSION_VALUE + ".checkpoint.json");

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

// Read the latest version of this id from the base table (partition key id, sort key version).
function getLatestVersion(id) {
  var latestParams = {
    TableName: TABLE_NAME,
    KeyConditionExpression: "#id = :id",
    ExpressionAttributeNames: { "#id": "id" },
    ExpressionAttributeValues: { ":id": id },
    ScanIndexForward: false, // descending by version - first result is the latest
    Limit: 1
  };
  return dynamodbDoc.send(new QueryCommand(latestParams)).then(function (data) {
    return data.Items && data.Items[0];
  });
}

// Look up the version currently indexed for this id in the 'item' index, if any.
function getIndexedVersion(id) {
  return client.get({
    index: 'item',
    id: id
  }).then(function (response) {
    return response.body._source.version;
  }).catch(function (err) {
    // A missing document rejects with a 404; treat that as "not indexed".
    if (err && err.meta && err.meta.statusCode === 404) {
      return undefined;
    }
    throw err;
  });
}

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
  }
  var itemTags = [];
  if (item.tags) {
    item.tags.forEach(function (tag) {
      itemTags.push(tag);
    });
  }
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

  if (item.title) indexBody.title = item.title;
  if (item.pageNumber) indexBody.pageNumber = item.pageNumber;
  if (item.recycled) indexBody.recycled = item.recycled;

  return indexBody;
}

// Index the latest version of the given id to the 'item' OpenSearch index.
function indexItem(latestItem) {
  var indexBody = buildIndexBody(latestItem);
  console.log(indexBody);

  return client.index({
    index: 'item',
    refresh: true,
    id: latestItem.id,
    body: indexBody
  }).catch(function (error) {
    console.log("Could not index item: <item.id>", latestItem.id);
    throw error;
  });
}

// For each id seen on the GSI, look up its latest version on the base table and index it. Retries on failure.
async function processItem(gsiItem) {
  for (var attempt = 1; ; attempt++) {
    try {
      var indexedVersion = await getIndexedVersion(gsiItem.id);
      if (indexedVersion !== undefined && indexedVersion >= gsiItem.version) {
        console.log("Already indexed with version >= current, skipping:", gsiItem.id, "indexedVersion:", indexedVersion, "version:", gsiItem.version);
        return;
      }

      var latestItem = await getLatestVersion(gsiItem.id);
      if (!latestItem) {
        console.error("Base table has no item for id:", gsiItem.id, "- skipping");
        number++;
        return;
      }

      await indexItem(latestItem);
      number++;
      console.log("number:", number);
      return;
    } catch (err) {
      var label = "Processing " + gsiItem.id + " failed";
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
