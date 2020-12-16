import * as fs from 'fs';
import * as rp from 'request-promise-native';
import { Headers } from 'request';

import { Config } from '../Config';

export enum DatabaseAdapterMode {
  TRANSACTION = 'transaction',
  IMMEDIATE = 'immediate',
};

export class DatabaseAdapter {
  private readonly config: Config;
  private readonly mode: DatabaseAdapterMode;
  private transactionQueue: rp.OptionsWithUri[] = [];

  get headers(): Headers {
    return {
      'Content-Type': 'application/json',
      ...this.authHeaders,
    };
  }

  get authHeaders(): Headers {
    return {
      'X-Koji-Project-Id': this.config.projectId,
      'X-Koji-Project-Token': this.config.projectToken,
    };
  }

  //////////////////////////////////////////////////////////////////////////////
  // Setup
  //////////////////////////////////////////////////////////////////////////////
  constructor(
    customConfig: Config | undefined,
    mode: DatabaseAdapterMode = DatabaseAdapterMode.IMMEDIATE,
  ) {
    if (customConfig) {
      this.config = customConfig;
    } else {
      if (!process.env.KOJI_PROJECT_ID || !process.env.KOJI_PROJECT_TOKEN) {
        throw new Error(`
          Couldn't find a KOJI_PROJECT_ID or KOJI_PROJECT_TOKEN in your environment.
          If you are accessing the SDK from outside of a KojiDatabase project, you need to either
          supply a custom configuration object, or export the KOJI_PROJECT_ID and
          KOJI_PROJECT_TOKEN in your script.
        `);
      }

      this.config = {
        projectId: process.env.KOJI_PROJECT_ID,
        projectToken: process.env.KOJI_PROJECT_TOKEN,
      };
    }

    this.mode = mode;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Store APIs
  //////////////////////////////////////////////////////////////////////////////
  public async get<T>(
    collection: string,
    documentName?: string | null,
  ): Promise<T> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/get'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentName,
      },
    };

    try {
      const response = await this.request(options);
      return response.document;
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error('Document not found');
      } else {
        throw new Error('Service error');
      }
    }
  }

  public async getCollections<T>(): Promise<T> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/getCollections'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {},
    };

    try {
      const response = await this.request(options);
      return response.collections;
    } catch (err) {
      throw new Error('Service error');
    }
  }

  public async search<T>(
    collection: string,
    queryKey: string,
    queryValue: string,
  ): Promise<T> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/search'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        queryKey,
        queryValue,
      },
    };

    try {
      const response = await this.request(options);
      return response.results;
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error('Document not found');
      } else {
        throw new Error('Service error');
      }
    }
  }

  public async getWhere<T>(
    collection: string,
    predicateKey: string,
    predicateOperation: string,
    predicateValue: string,
  ): Promise<T> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/get'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        predicate: {
          key: predicateKey,
          operation: predicateOperation,
          value: predicateValue,
        },
      },
    };

    try {
      const response = await this.request(options);
      return response.document;
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error('Document not found');
      } else {
        throw new Error('Service error');
      }
    }
  }

  public async getAll<T>(
    collection: string,
    documentNames: string[],
  ): Promise<T[]> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/getAll'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentNames,
      },
    };

    try {
      const response = await this.request(options);
      return response.results;
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error('Document not found');
      } else {
        throw new Error('Service error');
      }
    }
  }

  public async getAllWhere<T>(
    collection: string,
    predicateKey: string,
    predicateOperation: string,
    predicateValues: string[],
  ): Promise<T[]> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/getAllWhere'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        predicateKey,
        predicateOperation,
        predicateValues,
      },
    };

    try {
      const response = await this.request(options);
      return response.results;
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error('Document not found');
      } else {
        throw new Error('Service error');
      }
    }
  }

  public async set(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean|void> {
    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/set'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentName,
        documentBody,
      },
    };
    try {
      await this.request(options);
      if (this.mode === DatabaseAdapterMode.TRANSACTION) {
        return;
      }

      return true;
    } catch (e) {
      return false;
    }
  }

  public async update(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean|void> {
    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/update'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentName,
        documentBody,
      },
    };
    try {
      await this.request(options);
      if (this.mode === DatabaseAdapterMode.TRANSACTION) {
        return;
      }

      return true;
    } catch (e) {
      return false;
    }
  }

  public async arrayPush(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean|void> {
    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/update/push'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentName,
        documentBody,
      },
    };
    try {
      await this.request(options);
      if (this.mode === DatabaseAdapterMode.TRANSACTION) {
        return;
      }

      return true;
    } catch (e) {
      return false;
    }
  }

  public async arrayRemove(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean|void> {
    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/update/remove'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentName,
        documentBody,
      },
    };
    try {
      await this.request(options);
      if (this.mode === DatabaseAdapterMode.TRANSACTION) {
        return;
      }

      return true;
    } catch (e) {
      return false;
    }
  }

  public async delete(
    collection: string,
    documentName: string,
  ): Promise<boolean|void> {
    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/delete'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {
        collection,
        documentName,
      },
    };
    try {
      await this.request(options);
      if (this.mode === DatabaseAdapterMode.TRANSACTION) {
        return;
      }

      return true;
    } catch (e) {
      return false;
    }
  }

  // Upload a file
  public async uploadFile(
    path: string,
    filename?: string,
    contentType?: string,
  ): Promise<string|void> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/objectStore/upload'),
      method: 'POST',
      headers: this.authHeaders,
      formData: {
        file: {
          value: fs.createReadStream(path),
          options: {
            filename,
            contentType,
          },
        },
      },
    };

    try {
      const response = await this.request(options);

      const { url } = JSON.parse(response);
      return url;
    } catch (err) {
      throw new Error('Service error');
    }
  }

  // Generate a signed URL for uploading a file directly to the koji-cdn S3
  // bucket
  public async generateSignedUploadRequest(
    fileName: string,
  ): Promise<SignedUploadRequest> {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/objectStore/generateSignedRequest'),
      method: 'POST',
      headers: this.authHeaders,
      json: true,
      body: {
        fileName,
      },
    };

    try {
      const response = await this.request(options);
      const { url, signedRequest } = response;
      return {
        url,
        signedRequest,
      };
    } catch (err) {
      throw new Error('Service error');
    }
  }

  // Transcode an asset that has been uploaded to koji-cdn. This method returns
  // the URL of the transcoded asset, as well as a callback token that can be
  // used in conjunction with `getTranscodeStatus` to poll for transcode progress.
  // Transcoding videos usually completes in a few seconds, but can be longer
  // for longer videos. Use something like:
  //
  // await new Promise((resolve: any) => {
  //   const checkStatus = async () => {
  //     const { isFinished } = await database.getTranscodeStatus(callbackToken);
  //     if (isFinished) {
  //       resolve();
  //     } else {
  //       setTimeout(() => checkStatus(), 500);
  //     }
  //   };

  //   checkStatus();
  // });

  public async transcodeAsset(
    path: string,
    transcodeType: 'video+hls',
  ) {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/objectStore/transcode'),
      method: 'POST',
      headers: this.authHeaders,
      json: true,
      body: {
        path,
        type: transcodeType,
      },
    };

    try {
      const response = await this.request(options);
      const { url, callbackTokens } = response;
      return {
        url,
        callbackToken: callbackTokens[0],
      };
    } catch (err) {
      console.log(err);
      throw new Error('Service error');
    }
  }

  public async getTranscodeStatus(
    callbackToken: string,
  ) {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not available inside transaction');
    }

    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/objectStore/transcode/status'),
      method: 'POST',
      headers: this.authHeaders,
      json: true,
      body: {
        callbackToken,
      },
    };

    try {
      const response = await this.request(options);
      const { isResolved } = response;
      return {
        isFinished: isResolved,
      };
    } catch (err) {
      throw new Error('Service error');
    }
  }

  // Create a new transaction
  public beginTransaction(): DatabaseAdapter {
    return new DatabaseAdapter(this.config, DatabaseAdapterMode.TRANSACTION);
  }
  public async commitTransaction() {
    if (this.mode !== DatabaseAdapterMode.TRANSACTION) {
      throw new Error('not in a trasaction');
    }

    // Build one big request from everything
    const requestBody = this.transactionQueue.map(({ uri, body }) => ({
      uri,
      body,
    }));

    await rp({
      uri: this.buildUri('/v1/store/transaction'),
      method: 'POST',
      headers: this.authHeaders,
      json: true,
      body: {
        operations: requestBody,
      },
    });
  }

  //////////////////////////////////////////////////////////////////////////////
  // Helpers
  //////////////////////////////////////////////////////////////////////////////
  private buildUri(path: string): string {
    if (process.env.NODE_TEST) {
      return `http://localhost:3129${path}`;
    }
    return `https://database.api.gokoji.com${path}`;
  }

  private request(options: rp.OptionsWithUri) {
    if (this.mode === DatabaseAdapterMode.TRANSACTION) {
      // If we're in a transaction, batch all these requests and send them off
      // all at once when we commit the transaction.
      this.transactionQueue.push(options);
    } else {
      // Otherwise, send the request immediately and return the result
      return rp(options);
    }
  }
}

export interface SignedUploadRequest {
  url: string,
  signedRequest: {
    url: string,
    fields: {[index: string]: string};
  };
}
