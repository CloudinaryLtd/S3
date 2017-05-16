import assert from 'assert';

import { DummyRequestLogger } from '../helpers';
import { getReplicationConfiguration } from
    '../../../lib/api/apiUtils/bucket/bucketReplication';
import { replicationUtils } from
    '../../functional/aws-node-sdk/lib/utility/replication';

// Check for the expected error response code and status code.
function checkError(xml, expectedErr, cb) {
    const log = new DummyRequestLogger();
    getReplicationConfiguration(xml, log, err => {
        if (expectedErr === null) {
            assert.strictEqual(err, null, `expected no error but got '${err}'`);
        } else {
            assert(err[expectedErr], 'incorrect error response: should be ' +
                `'Error: ${expectedErr}' but got '${err}'`);
        }
        return cb();
    });
}

// Create replication configuration XML with an tag optionally omitted.
function createReplicationXML(missingTag) {
    const Role = missingTag === 'Role' ? '' :
        '<Role>arn:partition:service::account-id:resourcetype/resource</Role>';
    const ID = missingTag === 'ID' ? '' : '<ID>foo</ID>';
    const Prefix = missingTag === 'Prefix' ? '' : '<Prefix>foo</Prefix>';
    const Status = missingTag === 'Status' ? '' : '<Status>Enabled</Status>';
    const Bucket = missingTag === 'Bucket' ? '' :
        '<Bucket>arn:aws:s3:::destination-bucket</Bucket>';
    const StorageClass = missingTag === 'StorageClass' ? '' :
        '<StorageClass>STANDARD</StorageClass>';
    const Destination = missingTag === 'Destination' ? '' :
        `<Destination>${Bucket + StorageClass}</Destination>`;
    const Rule = missingTag === 'Rule' ? '' :
        `<Rule>${ID + Prefix + Status + Destination}</Rule>`;
    const content = missingTag === null ? '' : `${Role}${Rule}`;
    return '<ReplicationConfiguration ' +
            `xmlns="http://s3.amazonaws.com/doc/2006-03-01/">${content}` +
        '</ReplicationConfiguration>';
}

describe('\'getReplicationConfiguration\' function', () => {
    it('should not return error when putting valid XML', done =>
        checkError(createReplicationXML(), null, done));

    it('should not accept empty replication configuration', done =>
        checkError(createReplicationXML(null), 'MalformedXML', done));

    replicationUtils.requiredConfigProperties.forEach(prop => {
        // Note that the XML uses 'Rule' while the config object uses 'Rules'.
        const xmlTag = prop === 'Rules' ? 'Rule' : prop;
        const xml = createReplicationXML(xmlTag);

        it(`should not accept replication configuration without \'${prop}\'`,
            done => checkError(xml, 'MalformedXML', done));
    });

    replicationUtils.optionalConfigProperties.forEach(prop => {
        it(`should accept replication configuration without \'${prop}\'`,
            done => checkError(createReplicationXML(prop), null, done));
    });
});
