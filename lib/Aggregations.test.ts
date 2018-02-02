import "mocha"
import { assert } from "chai"
import apply from 'querycraft-to-function'
import toElastic from './toElastic'
import ESQueryAggregationSource, { getBucketsFromAggregationOutput, Bucket } from './Aggregations'
import { Aggregation, AbstractAggregationSource, BucketsAggregation, FilterAggregation, eq } from 'querycraft'
import { times, pluck, prop, range } from 'ramda'
import * as moment from 'moment'
import { Client } from 'elasticsearch'
import { testContacts, Contact } from '../test/testContacts'
import * as Debug from 'debug'

const debug = Debug('querycraft-to-elastic')

const testIndexName = 'querycraft-to-elastic-test-index'
const testContactsDocType = 'contact'

const wait = (delay: number) => new Promise(resolve => setTimeout(resolve, 1000))


function getIds<T extends { id: string }>(list: T[]){
    return list.map(({ id }) => id)
}

async function poll<T>(delay: number, fn: () => Promise<T>, retries = 10): Promise<T> {
    try {
        return await fn()
    } catch (e) {
        await wait(delay)
        if (retries === 0){
            throw e
        } else {
            return await poll(delay, fn, retries-1)
        }
    }
}
describe('aggregationFromPipeline',function(){
    let client: Client;
    let fieldIdMapFn = (fieldId: string) => {
        switch (fieldId) {
            case 'firstName':
            case 'lastName':
            case 'primaryEmail.value':
                return fieldId + '.keyword'
            default:
                return fieldId
        }
    }
    class ESAggsSource extends AbstractAggregationSource {
        private queryAggs = new ESQueryAggregationSource()
        async sink(aggregations: Aggregation[]){

            await client.indices.clearCache({
                index: testIndexName,
            })
            debug('INFO', JSON.stringify(aggregations, null, 2))

            const aggs = this.queryAggs.sink(aggregations)

            debug('INFO', JSON.stringify(aggs, null, 2))

            const result = await client.search({
                explain: true,
                index: testIndexName,
                body: { aggs, size: 0 }
            })

            debug('INFO', JSON.stringify(result, null, 2))

            await client.indices.clearCache({
                index: testIndexName,
            })

            return getBucketsFromAggregationOutput(result.aggregations)
        }
    }

    before('Connect to elasticseach client and set data', async function(){
        this.timeout(60000)
        client = new Client({ host: 'http://localhost:9200/'})

        await client.cluster.health({})
        debug('INFO', 'instance healthy')

        await client.indices.create({
            index: testIndexName,
        })
        debug('INFO', 'index created')

        const textFieldMapping = {
            type: "text",
            fields: {
                keyword: {
                    type: "keyword",
                    ignore_above: 256
                }
            }
        }

        await client.indices.putMapping({
            index: testIndexName,
            type: testContactsDocType,
            body: { properties: {
                id: { type: 'keyword' },
                assignedTo: { type: 'keyword' },
                firstName: textFieldMapping,
                lastName: textFieldMapping,
                createdAt: { type: 'date' },
                deletedAt: { type: 'date' },
                customFields: {
                    type: 'nested',
                    properties: {
                        id: { type: 'keyword'},
                    }
                },
                lists: {
                    type: 'nested',
                    properties: {
                        id: { type: 'keyword'}
                    }
                },
                vacancies: {
                    type: 'nested',
                    properties: {
                        id: { type: 'keyword'},
                        stage: {
                            properties: {
                                id: { type: 'keyword' }
                            }
                        },
                    }
                },
                primaryEmail: {
                    properties: {
                        id: { type: "keyword" },
                        value: textFieldMapping
                    }
                }
            } }
        })
        for (let contact of testContacts){
            debug('INFO', 'pushing contact', contact.id)
            await client.index({
                index: testIndexName,
                type: testContactsDocType,
                id: contact.id,
                body: contact
            })
        }

        const result = await poll(1000, async function(){
            try {
                assert.equal((await client.search({
                    explain: true,
                    index: testIndexName,
                    body: { size: 0 }
                })).hits.total, testContacts.length)
            } catch (e){
                debug(e)
                throw e
            }
        })
    })

    describe('BucketAggregations', function(){
        it('should let you group the object into buckets', async function(){
            const result = await new ESAggsSource()
            .pipe(new BucketsAggregation({
                fieldId: 'assignedTo'
            }))
            .sink()

            assert.sameMembers(pluck('id', result), ['', 'me', 'you', 'him', 'her'])
        })

        it('should let you group the object into buckets and restrict buckets to the given values', async function(){
            const result = await new ESAggsSource()
            .pipe(new BucketsAggregation({
                fieldId: 'assignedTo',
                values:  ['him', 'her']
            }))
            .sink()

            assert.sameMembers(pluck('id', result), ['him', 'her'])
        })

        it('should let you group the object into buckets, with sub-buckets', async function(){
            const result: Bucket[] = await new ESAggsSource()
            .pipe(new BucketsAggregation({
                fieldId: 'assignedTo',
                subBuckets: {
                    fieldId: 'vacancies',
                    subFieldIds: ['vacancy1', 'vacancy1', 'vacancy1'],
                    subFieldProp: 'stage.id'
                }
            }))
            .sink()

            assert.sameMembers(pluck('id', result), ['', 'me', 'you', 'him', 'her'])
            for (let bucket of result) {
                assert.sameMembers(pluck('id', bucket.buckets), ['stage1', 'stage2', 'stage3'])
            }
        })

        it('should let you group a number field by an interval into buckets', async function(){
            const result = await new ESAggsSource()
            .pipe(new BucketsAggregation({
                fieldId: 'customFields',
                subFieldIds: ['custom1'],
                subFieldProp: 'value',
                interval: 10
            }))
            .sink()

            assert.sameMembers(pluck('id', result), range(0, 10).map($ => $*10))
        })

        it('should let you group a date field by an date-interval into buckets', async function(){
            const result = await new ESAggsSource()
            .pipe(new BucketsAggregation({
                fieldId: 'createdAt',
                dateInterval: 'month',

            }))
            .sink()

            const estimateBucketCount = Math.ceil((Date.now() - 1488542197631)/(1000*60*60*24*31))

            assert.approximately(result.length, estimateBucketCount, 1)
        })
    })

    after('cleanup elasticseach test index', function(){
        return client.indices.delete({
            index: testIndexName
        })
    })
})