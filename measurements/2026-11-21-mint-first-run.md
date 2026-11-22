`python3 tests/mint/ratchet.py /tmp/mint-log/log.json tests/mint/passing.txt` (GitHub Actions run `44916617013`, 2026-11-21T14:57:46Z, image `minio/mint@sha256:08a05e68893c68be2a83b6f79556853ed6aa3c6c9e64c823a00853e4e55d2200`)

# MinIO Mint First Run Against AwanFS (2026-11-21)

- **Run:** [`44916617013`](https://github.com/iskandarnorazli/Awanfs/actions/runs/44916617013) on `ubuntu-latest`, PR #371 commit `a72712a`.
- **Raw JSON records:** 641 records across 13 active SDKs (line 40 banner `Dependency validation complete` skipped; `aws-sdk-java-v2` produced 0 entries because `ENABLE_HTTPS=0`; `versioning` panicked in `testLockingLegalhold` after 13 passing tests).
- **Aggregated unique tests:** 375 tests: 273 passed, 68 failed, 34 skipped.

## Per-SDK summary

| SDK | pass | fail | skip | total |
|---|---|---|---|---|
| aws-sdk-go-v2 | 3 | 1 | 0 | 4 |
| aws-sdk-php | 11 | 1 | 0 | 12 |
| aws-sdk-ruby | 11 | 1 | 0 | 12 |
| awscli | 8 | 1 | 0 | 9 |
| healthcheck | 0 | 1 | 0 | 1 |
| mc | 8 | 1 | 0 | 9 |
| minio-go | 49 | 33 | 9 | 91 |
| minio-java | 23 | 15 | 25 | 63 |
| minio-js | 102 | 12 | 0 | 114 |
| minio-py | 37 | 1 | 0 | 38 |
| s3cmd | 8 | 0 | 0 | 8 |
| s3select | 0 | 1 | 0 | 1 |
| versioning | 13 | 0 | 0 | 13 |
| **Total** | **273** | **68** | **34** | **375** |

## Failures grouped by feature

### `anonymous-and-presigned` (1 failed)

- `aws-sdk-go-v2::PresignedPut`: EOF

### `bucket-notifications` (2 failed)

- `minio-java::listenBucketNotification()`: error occurred
- `minio-js::listenBucketNotification(bucketName, prefix, suffix, events)`: AssertionError: expected 'Unexpected token \'<\', "<?xml vers".…' to match /A specified event is not supported for notifications./ at NotificationPoller.<anonym

### `buckets` (4 failed)

- `minio-go: testMakeBucketError::MakeBucket(bucketName, region)`: The specified location-constraint is not valid
- `minio-go: testMakeBucketRegions::MakeBucket(bucketName, region)`: The specified location-constraint is not valid
- `minio-java::removeBucket()`: error occurred
- `minio-js::"after all" hook in "functional tests"`: S3Error: The bucket you tried to delete is not empty at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parseRes

### `checksums` (1 failed)

- `minio-go: testPutMultipartObjectWithChecksums::PutObject(bucketName, objectName, reader,size, opts)`: The checksum you specified did not match what we received.

### `encryption` (2 failed)

- `minio-go: testEncryptedSSES3ToSSES3CopyObject::CopyObject(destination, source)`: This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location o
- `minio-go: testEncryptedSSES3ToUnencryptedCopyObject::CopyObject(destination, source)`: This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location o

### `listing` (1 failed)

- `minio-js::listObjects(bucketName, prefix, recursive)`: Error: listObjects lists 11 objects, expected 10 at Readable.<anonymous> (file:///mint/run/core/minio-js/minio-js/tests/functional/functional-tests.js:1704:23) 

### `minio-only` (14 failed)

- `healthcheck::testLivenessEndpoint`: GET /minio/health/live returned 403 Forbidden
- `minio-go: testBucketVersioningExcludedPrefixes::GetBucketVersioning/SetBucketVersioning`: MalformedXML
- `minio-java::addCannedPolicy()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::addUser()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::deleteUser()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::getUserInfo()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::listCannedPolicies()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::listUsers()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::putObjectFanOut()`: error occurred
- `minio-java::removeCannedPolicy()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::setPolicy()`: java.lang.RuntimeException: Request failed with response: <?xml version="1.0" encoding="UTF-8"?>
- `minio-java::uploadSnowballObjects()`: error occurred
- `minio-js::extensions.listObjectsV2WithMetadata(bucketName, prefix, recursive)`: Error: Expected tags: {"key1":"value1","key2":"value2"}, received: {} at Readable.<anonymous> (file:///mint/run/core/minio-js/minio-js/tests/functional/function
- `minio-py:test_set_get_bucket_versioning`: Traceback (most recent call last):

### `mint-vs-aws` (2 failed)

- `aws-sdk-php::abortMultipartupload ( array $params = [] )`: Error executing "AbortMultipartUpload" on "http://127.0.0.1:9000/aws-sdk-php-6b01b1eacec9c/obj1?uploadId=non-existent"; AWS HTTP error: Client error: `DELETE ht
- `minio-go: testListMultipartUpload::GetObject()`: want: minio.ListMultipartUploadsResult{Bucket:"minio-go-test-6e24culj1vrdjook", KeyMarker:"", UploadIDMarker:"", NextKeyMarker:"", NextUploadIDMarker:"", Encodi

### `multipart` (1 failed)

- `minio-go: testFPutObjectMultipart::FPutObject(bucketName, objectName, fileName, opts)`: The checksum you specified did not match what we received.

### `object-attributes-api` (2 failed)

- `minio-go: testGetObjectAttributes::GetObjectAttributes(ctx, bucketName, objectName, opts)`: The checksum you specified did not match what we received.
- `minio-java::getObjectAttributes()`: java.lang.NullPointerException: Cannot invoke "io.minio.messages.GetObjectAttributesOutput$ObjectParts.parts()" because the return value of "io.minio.messages.G

### `object-lock` (1 failed)

- `minio-js::putObjectRetention(bucketName, objectName, putOpts)`: S3Error: The requested sub-resource is not implemented. at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parse

### `objects` (3 failed)

- `minio-go: testRemoveMultipleObjectsWithResult::RemoveObjects(bucketName, objectsCh)`: Unexpected number of results
- `minio-java::putObject()`: error occurred
- `minio-js::"after all" hook in "Force Deletion of prefix"`: S3Error: The bucket you tried to delete is not empty at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parseRes

### `post-object` (6 failed)

- `aws-sdk-ruby::presignedPost(bucket_name,file_name,expires_in_sec,max_byte_size)`: Expected to be created object does NOT exist
- `minio-go: testPresignedPostPolicy::PresignedPostPolicy(policy)`: 405 Method Not Allowed
- `minio-go: testPresignedPostPolicyEmptyFileName::PresignedPostPolicy(policy)`: 405 Method Not Allowed
- `minio-go: testPresignedPostPolicyWrongFile::PresignedPostPolicy(policy)`: 405 Method Not Allowed
- `minio-java::getPresignedPostFormData()`: java.lang.Exception: failed to upload object. Response: Response{protocol=http/1.1, code=405, message=Method Not Allowed, url=http://127.0.0.1:9000/minio-java-t
- `minio-js::presignedPostPolicy(postPolicy, cb)`: Error: Method Not Allowed at Request.callback (minio-js/node_modules/superagent/src/node/index.js:901:17) at fn (minio-js/node_modules/superagent/src/node/index

### `s3-select` (3 failed)

- `minio-js::"after all" hook for "Remove Object post select of content:`: S3Error: The bucket you tried to delete is not empty at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parseRes
- `minio-js::selectObjectContent(bucketName, objectName, selectOpts)`: S3Error: The requested sub-resource is not implemented. at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parse
- `s3select:test_csv_input_quote_char::select_object_content(bucket_name, object_name, request)`: Traceback (most recent call last):

### `sigv2` (19 failed)

- `minio-go: testComposeMultipleSources::ComposeObject(destination, sourceList)`: Authentication failed
- `minio-go: testComposeObjectErrorCasesWrapper::ComposeObject(destination, sourceList)`: Authentication failed
- `minio-go: testCopyObjectV2::CopyObject(destination, source)`: Authentication failed
- `minio-go: testFGetObjectContextV2::FGetObject(ctx, bucketName, objectName,fileName)`: Authentication failed
- `minio-go: testFPutObjectContextV2::FPutObjectContext(ctx, bucketName, objectName, fileName, opts)`: Authentication failed
- `minio-go: testFPutObjectV2::FPutObject(bucketName, objectName, fileName, opts)`: Authentication failed
- `minio-go: testFunctionalV2::MakeBucket(bucketName, location)`: Authentication failed
- `minio-go: testGetObjectClosedTwiceV2::MakeBucket(bucketName, region)`: Authentication failed
- `minio-go: testGetObjectContextV2::GetObject(ctx, bucketName, objectName)`: Authentication failed
- `minio-go: testGetObjectReadAtFunctionalV2::GetObject(bucketName, objectName)`: Authentication failed
- `minio-go: testGetObjectReadSeekFunctionalV2::GetObject(bucketName, objectName)`: Authentication failed
- `minio-go: testMakeBucketErrorV2::MakeBucket(bucketName, region)`: Authentication failed
- `minio-go: testMakeBucketRegionsV2::MakeBucket(bucketName, region)`: Authentication failed
- `minio-go: testPutObject0ByteV2::PutObject(bucketName, objectName, reader, size, opts)`: Authentication failed
- `minio-go: testPutObjectContextV2::PutObject(ctx, bucketName, objectName, reader, size, opts)`: Authentication failed
- `minio-go: testPutObjectMetadataNonUSASCIIV2::PutObject(bucketName, objectName, reader, size, opts)`: Authentication failed
- `minio-go: testPutObjectNoLengthV2::PutObject(bucketName, objectName, reader, size, opts)`: Authentication failed
- `minio-go: testPutObjectsUnknownV2::PutObject(bucketName, objectName, reader,size,opts)`: Authentication failed
- `minio-go: testUserMetadataCopyingWrapper::CopyObject(destination, source)`: Authentication failed

### `storage-classes` (3 failed)

- `awscli::aws --endpoint-url http://127.0.0.1:9000 s3api head-object --bucket awscli-mint-test-bucket --key datafile-1-kB-copy`: StorageClass was not applied
- `mc::test_put_object_with_storage_class_error`
- `minio-go: testStorageClassInvalidMetadataPutObject::testStorageClassInvalidMetadataPutObject()`: PutObject with invalid storage class passed, was expected to fail

### `tagging` (1 failed)

- `minio-js::Set tags on a bucket`: S3Error: at Module.parseResponseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:111:13) at processTicksAndRejections (node:internal/pr

### `versioning` (2 failed)

- `minio-js::"after all" hook in "Force Deletion of objects with versions"`: S3Error: The bucket you tried to delete is not empty at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parseRes
- `minio-js::"after all" hook in "Force Deletion of prefix with versions"`: S3Error: The bucket you tried to delete is not empty at parseError (file:///mint/run/core/minio-js/minio-js/src/internal/xml-parser.ts:49:13) at Module.parseRes
