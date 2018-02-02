import { parseStatements } from './toElastic'
import { QueryBuilder, AbstractAggregationSource, AbstractAggregation, Aggregation, FilterAggregation, BucketsAggregation, AggregationJSON, BucketsJSON, FilterJSON, Statement } from 'querycraft'

/**
 * Get the maximum shared prefix between two strings
 *
 * @param {string} s
 * @param {string} t
 */
function sharedPrefix(s: string, t: string){
    if (typeof s === 'string' && typeof t === 'string') {
        let i = 0
        while (i < s.length && i < t.length && s[i] === t[i]) i++
        return s.slice(0, i)
    } else {
        return ''
    }
}

export interface ESFilterAggregationQuery {
    with_filter: {
        filter?: {
            term?: { [key: string]: string[] }
            terms?: { [key: string]: string[] }
            missing: string
        }
        aggs?: ESAggregationQuery
    }
}
export interface ESReverseNestedAggregationQuery {
    without_nested: {
        reverse_nested: {
            path:string
        }
        aggs: ESAggregationQuery
    }
}
export interface ESNestedAggregationQuery {
    with_nested: {
        nested: {
            path:string
        }
        aggs: ESAggregationQuery
    }
}

export interface ESGroupAggregationQuery {
    group: {
        aggs?: ESAggregationQuery
        date_histogram?: {
            field:string,
            interval:string
        }
        terms?: {
            field:string
            size:number
            missing:''
            include:string[]
        }
    }
}
export type ESAggregationQuery =
    | ESFilterAggregationQuery
    | ESNestedAggregationQuery
    | ESGroupAggregationQuery
    | ESReverseNestedAggregationQuery

/**
 * @param {ESAggregationQuery} _aggs continue aggregation pipeline
 * @param {BucketOptions} bucketOptions
 * @param {string} [nestedFieldId] if we are aggregating a nested field this
 * should be the id of that field in the top level document
 */
function getBucketAggregation(_aggs: ESAggregationQuery, bucketOptions: BucketsJSON, nestedFieldId: string = '', fieldMapFn = (fieldId:string) => fieldId): ESAggregationQuery {
    const subBucketOptions = bucketOptions.subBuckets
    const subFieldSuffixString = bucketOptions.subFieldProp ? '.' + bucketOptions.subFieldProp : '';
    const field = fieldMapFn(bucketOptions.fieldId + subFieldSuffixString);
    console.log(nestedFieldId)
    // The size of docs have to be equal to number of items we are aggregating
    const size = (bucketOptions.size ||
        (bucketOptions.values ? bucketOptions.values.length :
        bucketOptions.subFieldIds ? bucketOptions.subFieldIds.length : 10)) || 10;

    // Is this current grouping on a nested field?
    const isNested = bucketOptions.subFieldProp !== undefined
    const bestNesting = sharedPrefix(bucketOptions.fieldId, nestedFieldId).replace(/\.$/, '')

    const nextNestedFieldId = isNested ? bucketOptions.fieldId : bestNesting
            console.log(bestNesting, nestedFieldId)
    // We reverse nesting if we query a field that is not part of the current nested object
    const shouldReverseNesting = bestNesting.length < nestedFieldId.length

    const subAggs = subBucketOptions
        ? getBucketAggregation(_aggs, subBucketOptions, nextNestedFieldId, fieldMapFn)
        : _aggs

    let aggs: ESAggregationQuery = {
        group: bucketOptions.dateInterval ? {
            aggs: subAggs,
            date_histogram: {
                field,interval : bucketOptions.dateInterval,
            }
        }: bucketOptions.interval ? {
            aggs: subAggs,
            histogram: {
                field,interval : bucketOptions.interval,
            }
        }: {
            aggs: subAggs,
            terms: {
                field,
                include: bucketOptions.values,
                missing: '',
                size,
            }
        }
    } as ESGroupAggregationQuery;

    if (isNested){
        const filteredAggs = bucketOptions.subFieldIds ? {
            with_filter: {
                filter: { terms: { [bucketOptions.fieldId+'.id']: bucketOptions.subFieldIds } },
                aggs,
            }
        } as ESFilterAggregationQuery : aggs

        aggs = {
            with_nested: {
                nested: {
                    path: bucketOptions.fieldId
                },
                aggs: filteredAggs
            }
        } as ESAggregationQuery
    }

    if (shouldReverseNesting) {
        aggs = {
            without_nested: {
                reverse_nested: {
                    path: bestNesting || undefined
                },
                aggs
            }
        }
    }

    return aggs;
}

function getFilterAggregation(aggs: ESAggregationQuery, filter: FilterAggregation, fieldMapFn: (fieldId:string) => string): ESFilterAggregationQuery {
    return {
        with_filter: {
            filter: parseStatements(filter.getStatements(), fieldMapFn),
            aggs
        }
    }
}

function operationReducer(aggs: ESAggregationQuery, aggregation: Aggregation, fieldMapFn: (fieldId:string) => string): ESAggregationQuery {
    switch (aggregation.type) {
        case 'filter':
            return getFilterAggregation(aggs, aggregation as FilterAggregation, fieldMapFn)
        case 'buckets':
            return getBucketAggregation(aggs, aggregation as BucketsAggregation, '', fieldMapFn)
        default:
            return void 0
    }
}


export default class ESQueryAggregationSource extends AbstractAggregationSource {
    fieldMapFn: (fieldId:string) => string
    constructor(fieldMapFn?: (fieldId:string) => string){
        super()
        this.fieldMapFn = fieldMapFn || ($=>$)
    }
    sink(aggregations: Aggregation[]){
        let aggs: ESAggregationQuery;
        let i = aggregations.length
        while (i--){
            aggs = operationReducer(aggs, aggregations[i], this.fieldMapFn)
        }
        return aggs
    }
}

export interface ElasticSearchBucket {
    key: string
    doc_count: number
    group?: { buckets:ElasticSearchBucket[] }
    with_nested?: ElasticSearchBucket
    without_nested?: ElasticSearchBucket
    with_filter?: ElasticSearchBucket
}
/**
 * Unwrap the nested elasticsearch aggregation result tho get the data about
 * the buckets
 */
function getElasticSearchBucketsFromAggregationOutput(aggsResult: ElasticSearchBucket): ElasticSearchBucket[] {
    if (!aggsResult) return;
    return (aggsResult.group && aggsResult.group.buckets)
        || getElasticSearchBucketsFromAggregationOutput(aggsResult.with_nested)
        || getElasticSearchBucketsFromAggregationOutput(aggsResult.with_filter)
        || getElasticSearchBucketsFromAggregationOutput(aggsResult.without_nested)
        || []
}

export interface Bucket {
    id: string
    value: number
    buckets: Bucket[]
}
export function getBucketsFromAggregationOutput(aggsResult: ElasticSearchBucket): Bucket[] {
    return getElasticSearchBucketsFromAggregationOutput(aggsResult)
    .map(elasticSearchBucket => ({
        id: elasticSearchBucket.key,
        value: elasticSearchBucket.doc_count,
        buckets: getBucketsFromAggregationOutput(elasticSearchBucket)
    }));

}