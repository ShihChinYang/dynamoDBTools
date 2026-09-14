# itemVersionsV2 Stream Indexer - Summary

Added to `migration/lambda/`:

## `src/itemVersionsIndexer.js` (new)
Same indexing logic as the standalone tooling, adapted to this backend's conventions: uses `@opensearch-project/opensearch` and reads the endpoint from `process.env.searchEndpoint`. Only processes `MODIFY` stream records; indexes the record's `NewImage` (the whole item after modification) into the `itemversion` OpenSearch index.

## `package.json`
Added `@aws-sdk/util-dynamodb` (to unmarshal the stream's `NewImage`) and `@opensearch-project/opensearch`. Left `@elastic/elasticsearch` in place since the rest of `src/` isn't visible here to migrate those handlers too - that's a separate follow-up for the full ES -> OpenSearch swap.

## `serverless.yml`
New `WN_ItemVersionsIndexer` function, placed after `WN_Items`:

```yaml
WN_ItemVersionsIndexer:
  name: WN_ItemVersionsIndexer
  handler: src/itemVersionsIndexer.handler
  vpc: ${self:custom.vpc}
  environment:
    searchEndpoint: ${env:searchEndpoint}
  events:
    - stream:
        type: dynamodb
        arn: ${env:itemVersionsV2StreamArn}
        batchSize: 100
        startingPosition: LATEST
        filterPatterns:
          - eventName: [MODIFY]
        functionResponseType: ReportBatchItemFailures
```

- `filterPatterns` stops Lambda from even being invoked for INSERT/REMOVE (the in-code check stays too, as a backstop).
- `functionResponseType: ReportBatchItemFailures` is what makes the handler's `batchItemFailures` return value actually retry only the failed records instead of the whole batch.
- Followed the file's existing `${env:...}` pattern (used for `isLocal`, `lambdaTimeout`) for the two new deploy-time values, rather than hardcoding them.

No IAM statements were added for `dynamodb:DescribeStream`/`GetRecords`/`GetShardIterator`/`ListStreams` - Serverless Framework auto-generates those scoped to the specific stream ARN when it sees a `stream` event, so adding them manually would just be redundant broader permissions.

## Two things needed before this deploys

1. **Enable the stream** - `createItemVersionsV2Table.js` never set a `StreamSpecification`, so the table currently has no stream at all. Enable one (view type `NEW_IMAGE` or `NEW_AND_OLD_IMAGES`):
   ```
   aws dynamodb update-table --table-name com.bsafes.itemVersionsV2 \
     --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE
   ```

2. **Export the two env vars** before `serverless deploy`, e.g.:
   ```
   export searchEndpoint=<your OpenSearch endpoint>
   export itemVersionsV2StreamArn=$(aws dynamodb describe-table \
     --table-name com.bsafes.itemVersionsV2 --query 'Table.LatestStreamArn' --output text)
   ```
