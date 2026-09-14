// DynamoDB Streams handler for com.bsafes.itemVersionsV2.
// Only MODIFY events are indexed - the stream's NewImage is the whole item
// after the modification, so it's indexed as-is into the 'itemversion'
// OpenSearch index (same document shape as the migration/scanItemVersionsV2TimeStampIndex.js tool).
const { unmarshall } = require("@aws-sdk/util-dynamodb");
const { Client } = require("@opensearch-project/opensearch");

const client = new Client({ node: process.env.searchEndpoint });

exports.handler = async (event) => {
  var batchItemFailures = [];

  for (const record of event.Records) {
    if (record.eventName !== "MODIFY") {
      continue;
    }

    const newImage = record.dynamodb && record.dynamodb.NewImage;
    if (!newImage) {
      console.error("MODIFY record missing NewImage, skipping:", record.eventID);
      continue;
    }

    try {
      const item = unmarshall(newImage);
      await indexItem(item);
    } catch (error) {
      console.error("Failed to index record:", record.eventID, error);
      batchItemFailures.push({ itemIdentifier: record.dynamodb.SequenceNumber });
    }
  }

  // Only takes effect if the event source mapping has ReportBatchItemFailures
  // enabled (functionResponseType: ReportBatchItemFailures in serverless.yml).
  return { batchItemFailures: batchItemFailures };
};

async function indexItem(item) {
  const updatedBy = item.updatedBy ? item.updatedBy : item.owner;
  const displayName = item.displayName ? item.displayName : updatedBy;
  const indexBody = {
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

  const indexId = item.id + "-" + item.version;

  try {
    await client.index({
      index: "itemversion",
      id: indexId,
      body: indexBody
    });
    console.log("Indexed itemversion:", indexId);
  } catch (error) {
    console.error("Could not index item: <itemversion.indexId>", indexId);
    throw error;
  }
}
