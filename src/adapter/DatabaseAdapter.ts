import * as rp from 'request-promise-native';
import { Headers } from 'request';
import { StatusCodeError } from 'request-promise-native/errors';

import { Config } from '../Config';

export class DatabaseAdapter {

  private readonly config: Config;

  get headers(): Headers {
    return {
      'Content-Type': 'application/json',
      'X-Koji-Project-Id': this.config.projectId,
      'X-Koji-Project-Token': this.config.projectToken,
    };
  }

  //////////////////////////////////////////////////////////////////////////////
  // Setup
  //////////////////////////////////////////////////////////////////////////////
  constructor(customConfig: Config | undefined) {
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
  }

  //////////////////////////////////////////////////////////////////////////////
  // Store APIs
  //////////////////////////////////////////////////////////////////////////////
  public async set(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean> {
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
      await rp(options);
      return true;
    } catch (e) {
      return false;
    }
  }

  public async get<T>(
    collection: string,
    documentName?: string | null,
  ): Promise<T> {
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
      const response = await rp(options);
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
    const options: rp.OptionsWithUri = {
      uri: this.buildUri('/v1/store/getCollections'),
      method: 'POST',
      headers: this.headers,
      json: true,
      body: {},
    };

    try {
      const response = await rp(options);
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
      const response = await rp(options);
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
      const response = await rp(options);
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
      const response = await rp(options);
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
      const response = await rp(options);
      return response.results;
    } catch (err) {
      if (err.statusCode === 404) {
        throw new Error('Document not found');
      } else {
        throw new Error('Service error');
      }
    }
  }

  public async update(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean> {
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
      await rp(options);
      return true;
    } catch (e) {
      return false;
    }
  }

  public async arrayPush(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean> {
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
      await rp(options);
      return true;
    } catch (e) {
      return false;
    }
  }

  public async arrayRemove(
    collection: string,
    documentName: string,
    documentBody: any,
  ): Promise<boolean> {
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
      await rp(options);
      return true;
    } catch (e) {
      return false;
    }
  }

  public async delete(
    collection: string,
    documentName: string,
  ): Promise<boolean> {
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
      await rp(options);
      return true;
    } catch (e) {
      return false;
    }
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
}
