# QueryCraft-To-Elasticsearch-To-Elasticsearch
Converts a QueryCraft Filter Builder object into a function to filter arrays of objects.


[![NPM](https://nodei.co/npm/querycraft-to-elasticsearch.png)](https://npmjs.org/package/querycraft-to-elasticsearch)

[![npm version](https://badge.fury.io/js/querycraft-to-elasticsearch.svg)](https://badge.fury.io/js/querycraft-to-elasticsearch)
[![CircleCI](https://circleci.com/gh/BeameryHQ/QueryCraft-To-Elasticsearch.svg?style=shield)](https://circleci.com/gh/BeameryHQ/QueryCraft-To-Elasticsearch)
[![codecov](https://codecov.io/gh/BeameryHQ/QueryCraft-To-Elasticsearch/branch/master/graph/badge.svg)](https://codecov.io/gh/BeameryHQ/QueryCraft-To-Elasticsearch)
[![David deps](https://david-dm.org/BeameryHQ/QueryCraft-To-Elasticsearch.svg)](https://david-dm.org/BeameryHQ/QueryCraft-To-Elasticsearch)
[![Known Vulnerabilities](https://snyk.io/test/github/beameryhq/querycraft-to-elasticsearch/badge.svg)](https://snyk.io/test/github/beameryhq/querycraft-to-elasticsearch)

## Installation

```sh
npm install --save 'querycraft-to-elasticsearch'
```

## Examples

Suppose we have a collection of data that satisfies the interface

```ts
interface Contact {
    id: string
    'list': { id: string }[]
    firstName: string
    lastName: string
    email: string
    createdAt: Date
    customFields: { id: string, value: number }[]
    assignedTo?: string
}
```

If we want a query the describes the logic:-
```
    first 50 items where
        fistName is bob
        lastName is doyle OR is not set
        assignedTo is anything
        list has an item where id is item1
    sorted (in ascending order) by the value property of the customField where id is custom1
    created less than 5 days ago
```

We can build build it as easily as:-

```ts
import { FilterBuilder, eq, lt, neq, any, find, where } from 'querycraft'
import toElastic from 'querycraft-to-elasticsearch'

async function getContacts(filter: FilterBuilder){
    const result = await client.search({
        explain: true,
        index: testIndexName,
        body: toElastic(filter, fieldIdMapFn)
    })

    return  result.hits.hits.map(prop('_source')) as Contact[]
    // -> filtered list of contacts
}

const filter = new FilterBuilder()
.where('firstName', eq('bob'))
.where('list', find(where('id', eq('item1'))))
.where('lastName', any([
    eq('doyle'),
    eq(null)
]))
.where('createdAt', lt({ daysAgo: 5 }))
.where('assignedTo', neq(null))
.setSortFieldId('customFields', 'custom1', 'value')
.setSortDirection('ASC')
.setLimit(50)

getContacts(filter)
.then(console.log)
// -> filtered list of contacts

```
