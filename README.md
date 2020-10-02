# Koji Database SDK

**SDK for communicating between Koji template and its in-built database service.**

## Overview

Each Koji project includes a key-value store that you can use as a backend database for simple use cases, such as collecting information from users, aggregating survey or poll results, and creating leaderboards for games.

The @withkoji/database package enables you to implement a Koji database for the backend of your template.

## Installation

Install the package in the backend services of your Koji project.

```
npm install --save @withkoji/database
```

## Basic use

Import and instantiate the database SDK in then backend service.

```
import Database from '@withkoji/Database';
const database = new Database();
```

Set database entries in the database using the SDK.

```
const isAdded = await database.set('myCollection', 'myKey', {'myValue':1});
```

Get database entries from the database using the SDK.

```
const myEntry = await database.get('myCollection','myKey');
```

## Related resources

* [Package documentation](https://developer.withkoji.com/reference/packages/withkoji-database-package)
* [Vote counter template](http://developer.withkoji.com/docs/blueprints/vote-counter-blueprint)
* [Koji homepage](http://withkoji.com/)

## Contributions and questions

See the [contributions page](https://developer.withkoji.com/docs/about/contribute-koji-developers) on the developer site for info on how to make contributions to Koji repositories and developer documentation.

For any questions, reach out to the developer community or the `@Koji Team` on our [Discord server](https://discord.gg/eQuMJF6).
