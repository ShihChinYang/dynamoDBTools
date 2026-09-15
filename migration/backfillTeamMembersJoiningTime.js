const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, ScanCommand, UpdateCommand } = require("@aws-sdk/lib-dynamodb");

var REGION = "us-east-1";
var TABLE_NAME = "com.bsafes.teamMembersV2";

var useOpenBSafes = process.env.useOpenBSafes;
if (useOpenBSafes) {
  TABLE_NAME = "open." + TABLE_NAME;
}

var JOINING_TIME_STEP = 100;

// The joiningTime value to assign to the next item that needs one. Starts at
// JOINING_TIME_STEP and increases by JOINING_TIME_STEP after each item actually updated.
var nextJoiningTime = JOINING_TIME_STEP;

var ddbClient = new DynamoDBClient({ region: REGION });
var dynamodbDoc = DynamoDBDocumentClient.from(ddbClient);

var dryRun = process.argv.indexOf("--execute") === -1;

var params = {
  TableName: TABLE_NAME,
  FilterExpression: "attribute_not_exists(joiningTime)"
};

var scannedCount = 0;
var updatedCount = 0;

var MAX_ITEM_RETRIES = 3;
var ITEM_RETRY_DELAY_MS = 2000;

function delay(ms) {
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
}

// Set joiningTime on this item, but only if it's still missing (guards against
// a race with something else writing to the item between the scan and the update).
function setJoiningTime(teamId, memberId, joiningTime) {
  return dynamodbDoc.send(new UpdateCommand({
    TableName: TABLE_NAME,
    Key: { teamId: teamId, memberId: memberId },
    UpdateExpression: "SET joiningTime = :jt",
    ConditionExpression: "attribute_not_exists(joiningTime)",
    ExpressionAttributeValues: { ":jt": joiningTime }
  }));
}

async function processItem(item) {
  var id = item.teamId + "-" + item.memberId;
  var joiningTime = nextJoiningTime;

  if (dryRun) {
    console.log("[dry run] Would set joiningTime =", joiningTime, "on", id);
    updatedCount++;
    nextJoiningTime += JOINING_TIME_STEP;
    return;
  }

  for (var attempt = 1; ; attempt++) {
    try {
      await setJoiningTime(item.teamId, item.memberId, joiningTime);
      updatedCount++;
      nextJoiningTime += JOINING_TIME_STEP;
      console.log("Set joiningTime =", joiningTime, "on", id, "- updated:", updatedCount);
      return;
    } catch (err) {
      if (err.name === "ConditionalCheckFailedException") {
        console.log("joiningTime was already set on", id, "(by someone else) - skipping");
        return;
      }
      var label = "Updating " + id + " failed";
      if (attempt < MAX_ITEM_RETRIES) {
        console.log(label + " (attempt " + attempt + "/" + MAX_ITEM_RETRIES + "), retrying in " + ITEM_RETRY_DELAY_MS + "ms");
        await delay(ITEM_RETRY_DELAY_MS);
        continue;
      }
      console.error(label + " after " + MAX_ITEM_RETRIES + " attempts. Error JSON:", JSON.stringify(err, null, 2));
      throw err;
    }
  }
}

async function run() {
  if (dryRun) {
    console.log("Dry run (default): no items will be modified. Pass --execute to actually update items.");
  }

  while (true) {
    var data;
    try {
      data = await dynamodbDoc.send(new ScanCommand(params));
    } catch (err) {
      console.error("Unable to scan table. Error JSON:", JSON.stringify(err, null, 2));
      console.error("Scanned so far:", scannedCount, "updated so far:", updatedCount);
      process.exit(1);
    }

    var items = data.Items || [];
    scannedCount += items.length;
    for (var i = 0; i < items.length; i++) {
      await processItem(items[i]);
    }

    if (!data.LastEvaluatedKey) {
      break;
    }

    params.ExclusiveStartKey = data.LastEvaluatedKey;
    console.log("=========== Next page (matched so far: " + scannedCount + ", updated: " + updatedCount + ") ===========");
  }

  console.log("Done. Items missing joiningTime found:", scannedCount, "- updated:", updatedCount);
}

run().catch(function (err) {
  console.error("Unexpected error:", err);
  process.exit(1);
});
