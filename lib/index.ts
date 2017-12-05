import { FilterBuilder, QueryBuilder, Condition, OrderCondition } from 'querycraft'
import { mergeDeep } from './util'

/**
 * Converts an OrderCondition into an elsasticsearch range query
 * if you are working with days ago you flip the logic of lt/gt as gt
 * 3days ago is a date lower than the date 3 days ago
 * 
 * @param {OrderCondition} condition 
 * @param {string} rangeKey 
 * @returns 
 */
function getRangeQuery(condition: OrderCondition, rangeKey: string){
    const value =
        typeof condition.value === 'number' ||
        condition.value instanceof Date ||
        typeof condition.value === 'string' ?
            condition.value :
            `now-${condition.value.daysAgo}d/d`
    return { filter: [{ range: { [rangeKey]: { [condition.op.toLowerCase()]: value } } }] }
}
/**
 * Convert a condition on a field to an ElasticSearch boolean query param
 * 
 * @param {string} fieldId 
 * @param {Condition} condition 
 * @returns {*} 
 */
function conditionToBooleanQueryParam(fieldId: string, condition: Condition, fieldMapFn: (fieldId: string) => string): any {
    const mappedFieldId = fieldMapFn(fieldId) || fieldId
    switch (condition.op) {
        case 'EQ':
            return condition.value == null ?
                { must_not: [{ exists : { field : mappedFieldId } }] } :
                { filter: [{ term: { [mappedFieldId]: condition.value } } ] }
        case 'NEQ':
            return condition.value == null ?
                { filter: [{ exists : { field : mappedFieldId }}] } :
                { must_not: [{ term: { [mappedFieldId]: condition.value } }] }
        case 'LT': case 'GT': case 'LTE': case 'GTE':
            return getRangeQuery(condition, mappedFieldId)
        case 'ALL':
            return (condition.value as Condition[])
                .reduce((memo, condition) =>
                    mergeDeep(memo, conditionToBooleanQueryParam(mappedFieldId, condition, fieldMapFn)), {})
        case 'ANY':
            return { filter: [{ dis_max: {
                queries: (condition.value as Condition[])
                    .map(condition => conditionToBooleanQueryParam(mappedFieldId, condition, fieldMapFn))
                    .map(ESQuery => ESQuery.filter || [{ bool: ESQuery }])
                    .reduce((m,$)=> m.concat($), [{}]) } }] }
        case 'PREFIX':
            return { filter: [{ prefix: { [mappedFieldId]: condition.value } }] }
        case 'FIND':
            return { filter: [{ nested: {
                path: mappedFieldId,
                query: queryAsElastic(condition.value, fieldMapFn, {}, mappedFieldId)
            } }] }
        case 'NFIND':
            return { must_not: [{ nested: {
                path: mappedFieldId,
                query: queryAsElastic(condition.value, fieldMapFn, {}, mappedFieldId)
            } }] }
        default:
            throw new Error('Cannot generate ElasticSearch query for: ' + JSON.stringify(condition))
    }
}

/**
 * Convert a query object into an elsasticsearch query
 * 
 * @param {QueryBuilder} query
 * @param {*} [init] 
 * @returns {*} 
 */
function queryAsElastic(query: QueryBuilder, fieldMapFn: (fieldId: string) => string, init?:any, prefix?: string): any {
    return {
        bool: query.mapFieldConditions((fieldId, condition) => conditionToBooleanQueryParam(prefix ? prefix + '.' +fieldId : fieldId, condition, fieldMapFn))
            .reduce((memo, $) => mergeDeep(memo, $), init || {})
    }
}

/**
 * Convert a Filter object into the body of an ElasticSearch query
 * 
 * @export
 * @param {FilterBuilder} filter 
 * @returns 
 */
export default function toElastic(filter: FilterBuilder, maybeFieldMapFn?: (fieldId: string) => string){
    const fieldMapFn = maybeFieldMapFn || ($ => $)
    const filters = filter.getStatements().map(options => {
        const should = options.map(query => queryAsElastic(query, fieldMapFn))
        if (should.length === 1) return should[0]
        return { bool: { minimum_should_match: 1, should } }
    })
    const query = filters.length === 1 ? filters[0] : {
        bool: { filter: filters }
    }
    const sortFieldId = filter.getSortFieldId()
    const order = { ASC: 'asc', DESC: 'desc' }[filter.getSortDirection()]
    const missing = '_last'

    const size = filter.limit
    const sort:any = [{'id': { order }}]
    if (sortFieldId && sortFieldId !== 'id') {
        const sortFieldSubProp = filter.getSortFieldSubProp()
        const sortFieldSubId = filter.getSortFieldSubId()
        if (sortFieldSubProp && sortFieldSubId) {
            const path = fieldMapFn(sortFieldId + '.' + sortFieldSubProp)
            const nested_path = fieldMapFn(sortFieldId)
            const nested_id_path = fieldMapFn(sortFieldId + '.id')
            const nested_filter = { term: { [nested_id_path]: filter.getSortFieldSubId() } }
            sort.unshift({
                [path]: { order, missing, nested_path, nested_filter }
            })
        } else {
            sort.unshift({
                [fieldMapFn(sortFieldId)]: { order, missing }
            })
        }
    }
    return { query, size, sort }
}