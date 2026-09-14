const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const readline = require("readline");
const fs = require("fs");
const path = require("path");

var search = require('../../routes/search');
const { Client } = require('@opensearch-project/opensearch')
const client = new Client({ node: search.getElasticSearchEndPoint() })

var REGION = "us-east-1";
var TABLE_NAME = "com.bsafes.managedMembersV2";
var INDEX_NAME = "managedMembersV2TimeStamp"; // GSI: partition key currentKeyVersion, sort key createdTime

var useOpenBSafes = process.env.useOpenBSafes;
if (useOpenBSafes) {
  TABLE_NAME = "open." + TABLE_NAME;
}

var ddbClient = new DynamoDBClient({ region: REGION });
var dynamodbDoc = DynamoDBDocumentClient.from(ddbClient);

var keyVersionArg = process.argv[2];
if (keyVersionArg === undefined) {
  console.error("Missing required currentKeyVersion argument.");
  console.error("Usage: node scanManagedMembersTable.js <currentKeyVersion> [createdTime]");
  process.exit(1);
}
var KEY_VERSION_VALUE = Number(keyVersionArg);
if (!Number.isInteger(KEY_VERSION_VALUE)) {
  console.error("Invalid currentKeyVersion argument:", JSON.stringify(keyVersionArg), "- must be an integer.");
  console.error("Usage: node scanManagedMembersTable.js <currentKeyVersion> [createdTime]");
  process.exit(1);
}

// Optional: only consider managed members created after this createdTime.
var KEY_CONDITION = "currentKeyVersion = :kv";
var EXPRESSION_VALUES = { ":kv": KEY_VERSION_VALUE };

var timeStampArg = process.argv[3];
if (timeStampArg !== undefined) {
  var asNumber = Number(timeStampArg);
  var TIMESTAMP_VALUE = isNaN(asNumber) ? timeStampArg : asNumber;
  KEY_CONDITION += " AND createdTime > :ts";
  EXPRESSION_VALUES[":ts"] = TIMESTAMP_VALUE;
  console.log("Querying for currentKeyVersion:", KEY_VERSION_VALUE, "with createdTime >", TIMESTAMP_VALUE);
} else {
  console.log("Querying for currentKeyVersion:", KEY_VERSION_VALUE);
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
// Keyed by currentKeyVersion so runs for different key versions don't clobber each other's progress.
var CHECKPOINT_FILE = path.join(__dirname, "scanManagedMembersTable.currentKeyVersion-" + KEY_VERSION_VALUE + ".checkpoint.json");

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

// Read the full managedMember item from the base table using the masterId+memberName from the GSI.
function getFullItem(masterId, memberName) {
  return dynamodbDoc.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: { masterId: masterId, memberName: memberName }
  })).then(function (data) {
    return data.Item;
  });
}

// Check whether this id is already in the 'managedmember' index.
function alreadyIndexed(id) {
  return client.exists({
    index: 'managedmember',
    id: id
  }).then(function (response) {
    return response.body === true;
  });
}

// Index the full managedMember item to the 'managedmember' OpenSearch index (same shape scanManagedMembersTable.js used to build - the raw item).
function indexItem(managedMember) {
  console.log(managedMember);

  return client.index({
    index: 'managedmember',
    id: managedMember.id,
    body: managedMember
  }).catch(function (error) {
    console.log("Could not index managedMember: <managedMember.id>", managedMember.id);
    throw error;
  });
}

// Read the full item from the base table, then index it if not already indexed. Retries on failure.
async function processItem(gsiItem) {
  for (var attempt = 1; ; attempt++) {
    try {
      var exists = await alreadyIndexed(gsiItem.id);
      if (exists) {
        console.log("Already indexed, skipping:", gsiItem.id);
        return;
      }

      var fullItem = await getFullItem(gsiItem.masterId, gsiItem.memberName);
      if (!fullItem) {
        console.error("Base table has no item for masterId:", gsiItem.masterId, "memberName:", gsiItem.memberName, "- skipping");
        number++;
        return;
      }

      await indexItem(fullItem);
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
    console.log("createdTime argument provided - ignoring any saved checkpoint, starting fresh from that point.");
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
      console.log("[" + itemIndex + "] Processing masterId:", items[i].masterId, "memberName:", items[i].memberName);
      await processItem(items[i]);
    }

    if (items.length > 0) {
      // Remember our position: the GSI key (currentKeyVersion, createdTime) plus the
      // base table key (masterId, memberName) that DynamoDB requires for a GSI cursor.
      var lastItem = items[items.length - 1];
      params.ExclusiveStartKey = {
        currentKeyVersion: KEY_VERSION_VALUE,
        createdTime: lastItem.createdTime,
        masterId: lastItem.masterId,
        memberName: lastItem.memberName
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
