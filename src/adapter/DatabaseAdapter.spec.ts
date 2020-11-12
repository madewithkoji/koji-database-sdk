
import { Config } from '../Config';
import { DatabaseAdapter } from './DatabaseAdapter';
import { expect } from 'chai';
import 'mocha';

describe('Daemon API', () => {
  const testCollectionName = 'TEST_COLLECTION';
  const testDocumentName = 'TEST_DOCUMENT';

  const testDocument = {
    stringKey: 'testValue',
    numericKey: 100,
    booleanKey: true,
    arrayKey: ['one', 'two', 'three'],
  };

  const daemonApi = new DatabaseAdapter({
    projectId: 'a0a8222c-b014-4867-8319-e06b7025abfc',
    projectToken: 'test_secret_key',
  });

  it('should throw if a document does not exist', () => {
    daemonApi
      .get(testCollectionName, testDocumentName);
  });

  it('should create a document', () => {
    return daemonApi.set(testCollectionName, testDocumentName, testDocument)
      .then((success) => {
        expect(success).to.be.true;
      });
  });

  it('should read a document', () => {
    return daemonApi
      .get(testCollectionName, testDocumentName)
      .then((document: any) => {
        delete document._id;
        expect(document).to.deep.equal(testDocument);
      });
  });

  it('should list all collections for project', () => {
    return daemonApi.getCollections()
      .then((collections: any) => {
        expect(collections).to.include(testCollectionName);
      });
  });

  it('should search', () => {
    return daemonApi
      .search(testCollectionName, 'stringKey', 'test')
      .then((results: any) => {
        delete results[0]._id;
        expect(results[0]).to.deep.equal(testDocument);
      });
  });

  it('should read multiple documents', () => {
    return daemonApi
      .getAll(testCollectionName, [testDocumentName])
      .then((documents: any) => {
        delete documents[0]._id;
        expect(documents[0]).to.deep.equal(testDocument);
      });
  });

  it('should read multiple documents with predicate', () => {
    return daemonApi
      .getAllWhere(testCollectionName, 'stringKey', '=', ['testValue'])
      .then((documents: any) => {
        delete documents[0]._id;
        expect(documents[0]).to.deep.equal(testDocument);
      });
  });

  it('should read a document with a predicate', () => {
    return daemonApi
      .getWhere(testCollectionName, 'stringKey', '==', 'testValue')
      .then((document: any) => {
        delete document[0]._id;
        expect(document[0]).to.deep.equal(testDocument);
      });
  });

  it('should update a document', () => {
    return daemonApi.update(testCollectionName, testDocumentName, {
      stringKey: 'newValue',
    })
      .then(() => daemonApi.get(testCollectionName, testDocumentName))
      .then((document: any) => {
        expect(document.stringKey).to.eq('newValue');
      });
  });

  it('should append a field to an array', () => {
    return daemonApi.arrayPush(testCollectionName, testDocumentName, {
      arrayKey: 'four',
    })
      .then(() => daemonApi.get(testCollectionName, testDocumentName))
      .then((document: any) => {
        expect(document.arrayKey.length).to.eq(4);
      });
  });

  it('should remove a field from an array', () => {
    return daemonApi.arrayRemove(testCollectionName, testDocumentName, {
      arrayKey: 'four',
    })
      .then(() => daemonApi.get(testCollectionName, testDocumentName))
      .then((document: any) => {
        expect(document.arrayKey.length).to.eq(3);
      });
  });

  it('should delete a document', () => {
    return daemonApi.delete(testCollectionName, testDocumentName)
      .then((success) => {
        expect(success).to.be.true;
      });
  });

  it('should complete a transaction', async () => {
    const transaction = daemonApi.beginTransaction();
    await transaction.set(testCollectionName, testDocumentName, testDocument);
    await transaction.update(testCollectionName, testDocumentName, {
      stringKey: 'newValue',
    });
    await transaction.commitTransaction();

    const document: any = await daemonApi.get(testCollectionName, testDocumentName);
    expect(document.stringKey).to.eq('newValue');

    await daemonApi.delete(testCollectionName, testDocumentName);
  });

});
